/*
 * Copyright (C) 2015 Jörg Prante
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
package org.xbib.tools;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.xbib.elasticsearch.common.cron.CronExpression;
import org.xbib.elasticsearch.common.cron.CronThreadPoolExecutor;
import org.xbib.elasticsearch.support.client.Ingest;
import org.xbib.elasticsearch.support.client.ingest.IngestTransportClient;
import org.xbib.elasticsearch.support.client.mock.MockTransportClient;
import org.xbib.elasticsearch.support.client.transport.BulkTransportClient;
import org.xbib.metrics.MeterMetric;
import org.xbib.pipeline.AbstractPipeline;
import org.xbib.pipeline.Pipeline;
import org.xbib.pipeline.PipelineException;
import org.xbib.pipeline.PipelineProvider;
import org.xbib.pipeline.PipelineRequest;
import org.xbib.pipeline.simple.MetricSimplePipelineExecutor;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.collect.Lists.newLinkedList;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;

public abstract class Importer<T, R extends PipelineRequest, P extends Pipeline<T, R>>
        extends AbstractPipeline<SettingsPipelineElement, PipelineException>
        implements Runnable, CommandLineInterpreter {

    private final static Logger logger = LogManager.getLogger("importer");

    private final static SettingsPipelineElement pipelineElement = new SettingsPipelineElement();

    protected static Settings settings;

    protected static Queue<Settings> input;

    private MetricSimplePipelineExecutor<T, R, P> executor;

    private ThreadPoolExecutor threadPoolExecutor;

    private boolean done = false;

    private List<Future> futures;

    protected static Ingest ingest;

    private static String index;

    private static String concreteIndex;

    protected String getType() {
        return settings.get("type");
    }

    protected void setIndex(String newIndex) {
        index = newIndex;
    }

    protected String getIndex() {
        return index;
    }

    protected void setConcreteIndex(String newConcreteIndex) {
        concreteIndex = newConcreteIndex;
    }

    protected String getConcreteIndex() {
        return concreteIndex;
    }

    protected Ingest createIngest() {
        return settings.getAsBoolean("mock", false) ? new MockTransportClient() :
                "ingest".equals(settings.get("client")) ? new IngestTransportClient() :
                        new BulkTransportClient();
    }

    @Override
    public Importer<T, R, P> reader(String resourceName, InputStream in) {
        setSettings(settingsBuilder().loadFromStream(resourceName, in).build());
        return this;
    }

    public Importer<T, R, P> setSettings(Settings newSettings) {
        settings = newSettings;
        String statefile = settings.get("jdbc.statefile");
        if (statefile != null) {
            try {
                File file = new File(statefile);
                if (file.exists() && file.isFile() && file.canRead()) {
                    InputStream stateFileInputStream = new FileInputStream(file);
                    settings = settingsBuilder().put(settings).loadFromStream("statefile", stateFileInputStream).build();
                    logger.info("loaded state from {}", statefile);
                } else {
                    logger.warn("can't read from {}, skipped", statefile);
                }
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
        }
        return this;
    }

    @Override
    public void run(boolean bootstrap) {
        try {
            prepare();
            futures = schedule(settings);
            if (!futures.isEmpty()) {
                logger.debug("waiting for {} futures...", futures.size());
                for (Future future : futures) {
                    try {
                        Object o = future.get();
                        logger.debug("got future {}", o);
                    } catch (CancellationException e) {
                        logger.warn("schedule canceled");
                    } catch (InterruptedException e) {
                        logger.error(e.getMessage(), e);
                    } catch (ExecutionException e) {
                        logger.error(e.getMessage(), e);
                    }
                }
                logger.debug("futures complete");
            } else {
                execute();
            }
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
        } finally {
            try {
                cleanup();
                if (executor != null) {
                    executor.shutdown();
                }
            } catch (Throwable e) {
                logger.warn(e.getMessage(), e);
            }
        }
    }

    @Override
    public void run() {
        try {
            prepare();
            execute();
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
        } finally {
            try {
                cleanup();
                if (executor != null) {
                    executor.shutdown();
                }
            } catch (Throwable e) {
                logger.warn(e.getMessage(), e);
            }
        }
    }

    protected void prepare() throws IOException {
        input = new ConcurrentLinkedQueue<Settings>();
        input.add(settings);
        if (settings.getAsBoolean("parallel", false)) {
            for (int i = 1; i < settings.getAsInt("concurrency", 1); i++) {
                input.add(settings);
            }
        }
        logger.debug("input = {}", input);
        if (ingest == null) {
            Integer maxbulkactions = settings.getAsInt("max_bulk_actions", 1000);
            Integer maxconcurrentbulkrequests = settings.getAsInt("max_concurrent_bulk_requests",
                    Runtime.getRuntime().availableProcessors());
            ingest = createIngest();
            ingest.maxActionsPerBulkRequest(maxbulkactions)
                    .maxConcurrentBulkRequests(maxconcurrentbulkrequests);
        }
        createIndex(getIndex());
    }

    protected Importer<T, R, P> cleanup() throws IOException {
        logger.debug("cleanup (no op)");
        return this;
    }

    @Override
    public void close() throws IOException {
        logger.debug("close (no op)");
    }

    @Override
    public boolean hasNext() {
        return !done && !input.isEmpty();
    }

    @Override
    public SettingsPipelineElement next() {
        Settings settings = input.poll();
        done = settings == null;
        pipelineElement.set(settings);
        if (done) {
            logger.debug("done is true");
            return pipelineElement;
        }
        return pipelineElement;
    }

    @Override
    public void newRequest(Pipeline<MeterMetric, SettingsPipelineElement> pipeline, SettingsPipelineElement request) {
        try {
            process(request.get());
        } catch (Exception ex) {
            logger.error("error while getting next input: " + ex.getMessage(), ex);
        }
    }

    @Override
    public void error(Pipeline<MeterMetric, SettingsPipelineElement> pipeline, SettingsPipelineElement request, PipelineException error) {
        logger.error(error.getMessage(), error);
    }

    protected abstract PipelineProvider<P> pipelineProvider();

    protected abstract void process(Settings settings) throws Exception;

    protected List<Future> schedule(Settings settings) {
        List<Future> futures = newLinkedList();
        if (threadPoolExecutor != null) {
            logger.info("already scheduled");
            return futures;
        }
        String[] schedule = settings.getAsArray("schedule");
        Long seconds = settings.getAsTime("interval", TimeValue.timeValueSeconds(0)).seconds();
        if (schedule != null && schedule.length > 0) {
            Thread thread = new Thread(this);
            CronThreadPoolExecutor cronThreadPoolExecutor =
                    new CronThreadPoolExecutor(settings.getAsInt("threadpoolsize", 1));
            for (String cron : schedule) {
                futures.add(cronThreadPoolExecutor.schedule(thread, new CronExpression(cron)));
            }
            this.threadPoolExecutor = cronThreadPoolExecutor;
            logger.info("schedule with cron expressions {}", Arrays.asList(schedule));
        } else if (seconds > 0L) {
            Thread thread = new Thread(this);
            ScheduledThreadPoolExecutor scheduledThreadPoolExecutor =
                    new ScheduledThreadPoolExecutor(settings.getAsInt("threadpoolsize", 1));
            futures.add(scheduledThreadPoolExecutor.scheduleAtFixedRate(thread, 0L, seconds, TimeUnit.SECONDS));
            logger.info("schedule at fixed rate of {} seconds", seconds);
            this.threadPoolExecutor = scheduledThreadPoolExecutor;
        }
        return futures;
    }

    protected void execute() throws ExecutionException, InterruptedException {
        logger.debug("executing");
        executor = new MetricSimplePipelineExecutor<T, R, P>()
                .setConcurrency(settings.getAsInt("concurrency", 1))
                .setPipelineProvider(pipelineProvider())
                .prepare()
                .execute()
                .waitFor();
        logger.debug("execution completed");
    }

    public synchronized void shutdown() throws Exception {
        if (futures != null) {
            for (Future future : futures) {
                future.cancel(true);
            }
        }
        if (threadPoolExecutor != null) {
            threadPoolExecutor.shutdownNow();
            threadPoolExecutor = null;
        }
        if (executor != null) {
            executor.shutdown();
            executor = null;
        }
    }

    protected Importer createIndex(String index) throws IOException {
        if (index == null) {
            return this;
        }
        if (ingest.client() != null) {
            ingest.waitForCluster(ClusterHealthStatus.YELLOW, TimeValue.timeValueSeconds(30));
            try {
                if (settings.getAsStructuredMap().containsKey("index_settings")) {
                    String indexSettings = settings.get("index_settings");
                    InputStream indexSettingsInput = (indexSettings.startsWith("classpath:") ?
                            new URL(null, indexSettings, new ClasspathURLStreamHandler()) :
                            new URL(indexSettings)).openStream();
                    String indexMappings = settings.get("type_mapping", null);
                    InputStream indexMappingsInput = (indexMappings.startsWith("classpath:") ?
                            new URL(null, indexMappings, new ClasspathURLStreamHandler()) :
                            new URL(indexMappings)).openStream();
                    ingest.newIndex(getConcreteIndex(), getType(),
                            indexSettingsInput, indexMappingsInput);
                    indexSettingsInput.close();
                    indexMappingsInput.close();
                    ingest.startBulk(getConcreteIndex(), -1, 1000);
                }
            } catch (Exception e) {
                if (!settings.getAsBoolean("ignoreindexcreationerror", false)) {
                    throw e;
                } else {
                    logger.warn("index creation error, but configured to ignore", e);
                }
            }
        }
        return this;
    }

    protected String resolveAlias(String alias) {
        if (ingest.client() == null) {
            return alias;
        }
        GetAliasesResponse getAliasesResponse = ingest.client().admin().indices().prepareGetAliases(alias).execute().actionGet();
        if (!getAliasesResponse.getAliases().isEmpty()) {
            return getAliasesResponse.getAliases().keys().iterator().next().value;
        }
        return alias;
    }
}
