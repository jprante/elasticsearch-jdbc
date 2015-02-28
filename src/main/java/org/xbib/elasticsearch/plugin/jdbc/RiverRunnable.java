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
package org.xbib.elasticsearch.plugin.jdbc;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.unit.TimeValue;
import org.xbib.elasticsearch.action.plugin.jdbc.state.get.GetRiverStateRequestBuilder;
import org.xbib.elasticsearch.action.plugin.jdbc.state.get.GetRiverStateResponse;
import org.xbib.elasticsearch.plugin.jdbc.pipeline.Pipeline;
import org.xbib.elasticsearch.plugin.jdbc.pipeline.PipelineProvider;
import org.xbib.elasticsearch.plugin.jdbc.pipeline.PipelineRequest;
import org.xbib.elasticsearch.plugin.jdbc.pipeline.executor.MetricSimplePipelineExecutor;
import org.xbib.elasticsearch.plugin.jdbc.state.RiverState;
import org.xbib.elasticsearch.river.jdbc.RiverContext;
import org.xbib.elasticsearch.river.jdbc.RiverFlow;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * A runnable that spawns a single step of river operation.
 *
 * @param <T> a pipeline result type
 * @param <R> a pipeline request type
 * @param <P> a pipeline type
 */
public class RiverRunnable<T, R extends PipelineRequest, P extends Pipeline<T, R>>
        implements Runnable {

    private final static ESLogger logger = ESLoggerFactory.getLogger("river.jdbc.RiverThread");

    private final RiverFlow riverFlow;

    private final List<Map<String, Object>> input;

    private MetricSimplePipelineExecutor executor;

    private List<RiverPipeline> pipelines;

    private ScheduledThreadPoolExecutor metricsThreadPoolExecutor;

    private ScheduledThreadPoolExecutor suspensionThreadPoolExecutor;

    private volatile Thread metricsThread;

    private volatile Thread suspensionThread;

    public RiverRunnable(RiverFlow riverFlow, List<Map<String, Object>> input) {
        this.riverFlow = riverFlow;
        this.input = input;
        this.pipelines = new LinkedList<RiverPipeline>();
    }

    /**
     * Before the pipelines are executed, put the river definitions on the queue
     *
     * @throws IOException
     */
    protected void beforePipelineExecutions() throws IOException {
        for (Map<String, Object> definition : input) {
            RiverContext riverContext = riverFlow.newRiverContext();
            riverContext.setDefinition(definition);
            riverFlow.getQueue().offer(riverContext);
        }
        if (riverFlow.isMetricThreadEnabled()) {
            this.metricsThread = new Thread(new MetricThread());
            metricsThread.setDaemon(true);
            // schedule river metrics thread
            long metricsInterval = riverFlow.getSettings().getAsTime("metrics_interval", TimeValue.timeValueSeconds(60)).getSeconds();
            this.metricsThreadPoolExecutor = new ScheduledThreadPoolExecutor(1);
            metricsThreadPoolExecutor.scheduleAtFixedRate(metricsThread, 10L, metricsInterval, TimeUnit.SECONDS);
            logger.info("scheduled metrics thread at {} seconds", metricsInterval);
        }
        if (riverFlow.isSuspensionThreadEnabled()) {
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
        logger.debug("river flow {} thread is starting", riverFlow);
        try {
            beforePipelineExecutions();
            this.executor = new MetricSimplePipelineExecutor<T, R, P>(riverFlow.getMetric())
                    .setConcurrency(riverFlow.getSettings().getAsInt("concurrency", 1))
                    .setPipelineProvider(new PipelineProvider<P>() {
                        @Override
                        @SuppressWarnings("unchecked")
                        public P get() {
                            RiverPipeline pipeline = new RiverPipeline(riverFlow);
                            pipelines.add(pipeline);
                            return (P) pipeline;
                        }
                    });
            executor.prepare()
                    .execute()
                    .waitFor();
            executor.shutdown();
        } catch (InterruptedException e) {
            for (RiverPipeline pipeline : pipelines) {
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
        logger.debug("river flow {} thread is finished", riverFlow);
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
     * Should be called regularly to check for a new river state, if the river is suspended.
     * If so, enter suspension mode. This is a bit tricky for a list of pipelines: first, all
     * pipeline sources and mouths are suspended. Then, a state change is monitored for resuming.
     * At last, all pipeline sources and mouths are resumed.
     */
    public void checkForSuspension() {
        GetRiverStateRequestBuilder riverStateRequestBuilder = new GetRiverStateRequestBuilder(riverFlow.getClient().admin().cluster())
                .setRiverName(riverFlow.getRiverName().getName())
                .setRiverType(riverFlow.getRiverName().getType());
        GetRiverStateResponse riverStateResponse = riverStateRequestBuilder.execute().actionGet();
        RiverState riverState = riverStateResponse.getRiverState();
        if (riverState.isSuspended()) {
            // suspend all sources and mouths
            for (RiverPipeline pipeline : pipelines) {
                RiverContext riverContext = pipeline.getRiverContext();
                try {
                    if (riverContext.getRiverSource() != null) {
                        riverContext.getRiverSource().suspend();
                    }
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
                try {
                    if (riverContext.getRiverMouth() != null) {
                        riverContext.getRiverMouth().suspend();
                    }
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            }
            // wait for resume
            try {
                do {
                    riverStateRequestBuilder = new GetRiverStateRequestBuilder(riverFlow.getClient().admin().cluster())
                            .setRiverName(riverFlow.getRiverName().getName())
                            .setRiverType(riverFlow.getRiverName().getType());
                    riverStateResponse = riverStateRequestBuilder.execute().actionGet();
                    riverState = riverStateResponse.getRiverState();
                    if (riverState.isSuspended()) {
                        Thread.sleep(1000L);
                    }
                } while (riverState.isSuspended());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.warn("interrupted");
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
            // resume all sources and mouths
            for (RiverPipeline pipeline : pipelines) {
                RiverContext riverContext = pipeline.getRiverContext();
                try {
                    if (riverContext.getRiverSource() != null) {
                        riverContext.getRiverSource().resume();
                    }
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
                try {
                    if (riverContext.getRiverMouth() != null) {
                        riverContext.getRiverMouth().resume();
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
            for (RiverPipeline pipeline : pipelines) {
                riverFlow.logMetrics(pipeline.getRiverContext(), "pipeline " + pipeline + " is running");
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
