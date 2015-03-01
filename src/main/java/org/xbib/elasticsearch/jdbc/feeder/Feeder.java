/*
 * Copyright (C) 2014 Jörg Prante
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.xbib.elasticsearch.jdbc.feeder;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.unit.TimeValue;
import org.xbib.elasticsearch.action.jdbc.task.get.GetTaskRequestBuilder;
import org.xbib.elasticsearch.action.jdbc.task.get.GetTaskResponse;
import org.xbib.elasticsearch.common.pipeline.Pipeline;
import org.xbib.elasticsearch.common.pipeline.PipelineProvider;
import org.xbib.elasticsearch.common.pipeline.PipelineRequest;
import org.xbib.elasticsearch.common.pipeline.executor.MetricSimplePipelineExecutor;
import org.xbib.elasticsearch.common.state.State;
import org.xbib.elasticsearch.jdbc.strategy.Context;
import org.xbib.elasticsearch.jdbc.strategy.Flow;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * A feeder spawns a single step of operation.
 *
 * @param <T> a pipeline result type
 * @param <R> a pipeline request type
 * @param <P> a pipeline type
 */
public class Feeder<T, R extends PipelineRequest, P extends Pipeline<T, R>>
        implements Runnable {

    private final static ESLogger logger = ESLoggerFactory.getLogger("jdbc");

    private final Flow flow;

    private final List<Map<String, Object>> input;

    private MetricSimplePipelineExecutor executor;

    private List<FeederPipeline> pipelines;

    private ScheduledThreadPoolExecutor metricsThreadPoolExecutor;

    private ScheduledThreadPoolExecutor suspensionThreadPoolExecutor;

    private volatile Thread metricsThread;

    private volatile Thread suspensionThread;

    public Feeder(Flow flow, List<Map<String, Object>> input) {
        this.flow = flow;
        this.input = input;
        this.pipelines = new LinkedList<FeederPipeline>();
    }

    /**
     * Before the pipelines are executed, prepare something.
     *
     * @throws IOException if this method fails
     */
    protected void beforePipelineExecutions() throws IOException {
        // fill queue
        for (Map<String, Object> definition : input) {
            Context context = flow.newContext();
            context.setDefinition(definition);
            flow.getQueue().offer(context);
        }
        if (flow.isMetricThreadEnabled()) {
            this.metricsThread = new Thread(new MetricThread());
            metricsThread.setDaemon(true);
            // schedule metrics thread
            long metricsInterval = flow.getSettings().getAsTime("metrics_interval", TimeValue.timeValueSeconds(60)).getSeconds();
            this.metricsThreadPoolExecutor = new ScheduledThreadPoolExecutor(1);
            metricsThreadPoolExecutor.scheduleAtFixedRate(metricsThread, 10L, metricsInterval, TimeUnit.SECONDS);
            logger.info("scheduled metrics thread at {} seconds", metricsInterval);
        }
        if (flow.isSuspensionThreadEnabled()) {
            this.suspensionThread = new Thread(new SuspendThread());
            suspensionThread.setDaemon(true);
            // schedule suspension thread
            long suspensionCheckInterval = TimeValue.timeValueSeconds(1).getSeconds();
            this.suspensionThreadPoolExecutor = new ScheduledThreadPoolExecutor(1);
            suspensionThreadPoolExecutor.scheduleAtFixedRate(suspensionThread, 10L,
                    suspensionCheckInterval,
                    TimeUnit.SECONDS);
            logger.info("scheduled suspend check thread at {} seconds", suspensionCheckInterval);
        }
    }

    @Override
    public void run() {
        logger.debug("flow {} thread is starting", flow);
        try {
            beforePipelineExecutions();
            this.executor = new MetricSimplePipelineExecutor<T, R, P>(flow.getMetric())
                    .setConcurrency(flow.getSettings().getAsInt("concurrency", 1))
                    .setPipelineProvider(new PipelineProvider<P>() {
                        @Override
                        @SuppressWarnings("unchecked")
                        public P get() {
                            FeederPipeline pipeline = new FeederPipeline(flow);
                            pipelines.add(pipeline);
                            return (P) pipeline;
                        }
                    });
            executor.prepare()
                    .execute()
                    .waitFor();
            executor.shutdown();
        } catch (InterruptedException e) {
            for (FeederPipeline pipeline : pipelines) {
                pipeline.setInterrupted(true);
            }
            Thread.currentThread().interrupt();
            logger.warn("interrupted");
        } catch (Throwable t) {
            logger.error(t.getMessage(), t);
        } finally {
            if (executor != null) {
                executor.shutdown();
            }
            closeMetricThread();
            closeSuspensionThread();
        }
        logger.debug("flow {} thread is finished", flow);
    }

    private void closeMetricThread() {
        if (metricsThreadPoolExecutor != null) {
            logger.debug("shutting down metrics thread scheduler");
            metricsThreadPoolExecutor.shutdownNow();
            metricsThreadPoolExecutor = null;
        }
        if (metricsThread != null) {
            metricsThread.interrupt();
        }
    }

    private void closeSuspensionThread() {
        if (suspensionThreadPoolExecutor != null) {
            logger.debug("shutting down suspension thread scheduler");
            suspensionThreadPoolExecutor.shutdownNow();
            suspensionThreadPoolExecutor = null;
        }
        if (suspensionThread != null) {
            suspensionThread.interrupt();
        }
    }

    /**
     * Should be called regularly to check for a new state, if the feed is suspended.
     * If so, enter suspension mode. This is a bit tricky for a list of pipelines: first, all
     * pipeline sources and mouths are suspended. Then, a state change is monitored for resuming.
     * At last, all pipeline sources and mouths are resumed.
     */
    public void checkForSuspension() {
        GetTaskRequestBuilder stateRequestBuilder = new GetTaskRequestBuilder(flow.getClient().admin().cluster())
                .setName(flow.getName());
        GetTaskResponse stateResponse = stateRequestBuilder.execute().actionGet();
        State state = stateResponse.getState();
        if (state.isSuspended()) {
            // suspend all sources and mouths
            for (FeederPipeline pipeline : pipelines) {
                Context context = pipeline.getContext();
                try {
                    if (context.getSource() != null) {
                        context.getSource().suspend();
                    }
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
                try {
                    if (context.getMouth() != null) {
                        context.getMouth().suspend();
                    }
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            }
            // wait for resume
            try {
                do {
                    stateRequestBuilder = new GetTaskRequestBuilder(flow.getClient().admin().cluster())
                            .setName(flow.getName());
                    stateResponse = stateRequestBuilder.execute().actionGet();
                    state = stateResponse.getState();
                    if (state.isSuspended()) {
                        Thread.sleep(1000L);
                    }
                } while (state.isSuspended());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.warn("interrupted");
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
            // resume all sources and mouths
            for (FeederPipeline pipeline : pipelines) {
                Context context = pipeline.getContext();
                try {
                    if (context.getSource() != null) {
                        context.getSource().resume();
                    }
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
                try {
                    if (context.getMouth() != null) {
                        context.getMouth().resume();
                    }
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }
    }

    class MetricThread implements Runnable {

        @Override
        public void run() {
            for (FeederPipeline pipeline : pipelines) {
                flow.logMetrics(pipeline.getContext(), "pipeline " + pipeline + " is running");
            }
        }
    }

    class SuspendThread implements Runnable {

        @Override
        public void run() {
            checkForSuspension();
        }
    }

}
