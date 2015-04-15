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
package org.xbib.elasticsearch.plugin.jdbc.river;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.metrics.MeterMetric;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.loader.JsonSettingsLoader;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.xbib.elasticsearch.action.plugin.jdbc.state.delete.DeleteRiverStateAction;
import org.xbib.elasticsearch.action.plugin.jdbc.state.delete.DeleteRiverStateRequest;
import org.xbib.elasticsearch.action.plugin.jdbc.state.get.GetRiverStateRequestBuilder;
import org.xbib.elasticsearch.action.plugin.jdbc.state.get.GetRiverStateResponse;
import org.xbib.elasticsearch.plugin.jdbc.RiverRunnable;
import org.xbib.elasticsearch.plugin.jdbc.client.Ingest;
import org.xbib.elasticsearch.plugin.jdbc.client.IngestFactory;
import org.xbib.elasticsearch.plugin.jdbc.client.node.BulkNodeClient;
import org.xbib.elasticsearch.plugin.jdbc.cron.CronExpression;
import org.xbib.elasticsearch.plugin.jdbc.cron.CronThreadPoolExecutor;
import org.xbib.elasticsearch.plugin.jdbc.execute.RunnableRiver;
import org.xbib.elasticsearch.plugin.jdbc.state.RiverState;
import org.xbib.elasticsearch.plugin.jdbc.state.StatefulRiver;
import org.xbib.elasticsearch.plugin.jdbc.util.RiverServiceLoader;
import org.xbib.elasticsearch.river.jdbc.RiverFlow;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.collect.Lists.newLinkedList;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * JDBC river
 */
public class JDBCRiver extends AbstractRiverComponent implements StatefulRiver, RunnableRiver {

    private final static ESLogger logger = ESLoggerFactory.getLogger("river.jdbc.JDBCRiver");

    private Client client;

    private RiverFlow riverFlow;

    private List<Map<String, Object>> definitions;

    private ThreadPoolExecutor threadPoolExecutor;

    private volatile Thread riverThread;

    private volatile boolean closed;

    private String[] schedule;

    private List<Future<?>> futures;

    private Long interval;

    @Inject
    @SuppressWarnings({"unchecked"})
    public JDBCRiver(RiverName riverName, RiverSettings riverSettings, Client client) {
        super(riverName, riverSettings);
        this.client = client;
        if (!riverSettings.settings().containsKey("jdbc")) {
            throw new IllegalArgumentException("no 'jdbc' settings in river settings?");
        }
        try {
            Map<String, String> loadedSettings = new JsonSettingsLoader()
                    .load(jsonBuilder().map(riverSettings.settings()).string());
            Settings settings = settingsBuilder().put(loadedSettings).build();
            String strategy = XContentMapValues.nodeStringValue(riverSettings.settings().get("strategy"), "simple");
            this.schedule = settings.getAsArray("schedule");
            this.interval = settings.getAsTime("interval", TimeValue.timeValueSeconds(0)).seconds();
            this.definitions = newLinkedList();
            Object definition = riverSettings.settings().get("jdbc");
            if (definition instanceof Map) {
                Map<String, Object> map = (Map<String, Object>) definition;
                definitions.add(map);
                // legacy mode: check for "strategy", "schedule", "interval" in the "jdbc" definition part
                if (map.containsKey("strategy")) {
                    strategy = map.get("strategy").toString();
                }
                if (map.containsKey("schedule")) {
                    this.schedule = settingsBuilder().put(new JsonSettingsLoader()
                            .load(jsonBuilder().map(map).string())).build().getAsArray("schedule");
                }
                if (map.containsKey("interval")) {
                    this.interval = XContentMapValues.nodeLongValue(map.get("interval"), 0L);
                }
            }
            if (definition instanceof List) {
                definitions.addAll((List<Map<String, Object>>) definition);
            }
            this.riverFlow = RiverServiceLoader.newRiverFlow(strategy);
            logger.debug("strategy {}: river flow class {} found, settings = {}",
                    strategy, riverFlow.getClass().getName(), settings.getAsMap());
            riverFlow.setRiverName(riverName)
                    .setSettings(settings)
                    .setClient(client)
                    .setIngestFactory(createIngestFactory(settings))
                    .setMetric(new MeterMetric(Executors.newScheduledThreadPool(1), TimeUnit.SECONDS))
                    .setQueue(new ConcurrentLinkedQueue<Map<String, Object>>());
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }

    /**
     * Called from Elasticsearch after recovery of indices when a node has initialized.
     */
    @Override
    public void start() {
        closed = false;
        this.riverThread = EsExecutors.daemonThreadFactory(settings.globalSettings(),
                "river(" + riverName().getType() + "/" + riverName().getName() + ")")
                .newThread(new RiverRunnable(riverFlow, definitions));
        this.futures = schedule(riverThread);
        // we do not wait for futures here, instead, we return to Elasticsea
    }

    /**
     * Called form Elasticsearch when river is deleted.
     */
    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        if (threadPoolExecutor != null) {
            logger.debug("shutting down river thread scheduler");
            threadPoolExecutor.shutdownNow();
            threadPoolExecutor = null;
        }
        if (riverThread != null) {
            logger.debug("interrupting river thread");
            riverThread.interrupt();
        }
        logger.debug("shutting down river flow");
        try {
            riverFlow.shutdown();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        logger.info("river closed [{}/{}]", riverName.getType(), riverName.getName());

        // delete state
        DeleteRiverStateRequest riverStateRequest = new DeleteRiverStateRequest();
        riverStateRequest.setRiverName(riverName.getName()).setRiverType(riverName.getType());
        client.admin().cluster().execute(DeleteRiverStateAction.INSTANCE, riverStateRequest).actionGet();
        logger.info("river state deleted [{}/{}]", riverName.getType(), riverName.getName());
    }

    /**
     * Execute river once from execute API, no matter if schedule/interval is defined.
     * If river is running, prevoius instance are interrupted and closed.
     */
    public void run() {
        if (!closed) {
            close();
        }
        this.riverThread = EsExecutors.daemonThreadFactory(settings.globalSettings(),
                "river(" + riverName().getType() + "/" + riverName().getName() + ")")
                .newThread(new RiverRunnable(riverFlow, definitions));
        this.threadPoolExecutor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>());
        futures.add(threadPoolExecutor.submit(riverThread));
        logger.info("started river instance for single run");
    }

    private List<Future<?>> schedule(Thread thread) {
        Settings settings = riverFlow.getSettings();
        List<Future<?>> futures = newLinkedList();
        if (schedule != null && schedule.length > 0) {
            CronThreadPoolExecutor cronThreadPoolExecutor =
                    new CronThreadPoolExecutor(settings.getAsInt("threadpoolsize", 1));
            for (String cron : schedule) {
                futures.add(cronThreadPoolExecutor.schedule(thread, new CronExpression(cron)));
            }
            this.threadPoolExecutor = cronThreadPoolExecutor;
            logger.info("scheduled river instance with cron expressions {}", Arrays.asList(schedule));
        } else if (interval > 0L) {
            ScheduledThreadPoolExecutor scheduledthreadPoolExecutor = new ScheduledThreadPoolExecutor(settings.getAsInt("threadpoolsize", 4));
            futures.add(scheduledthreadPoolExecutor.scheduleAtFixedRate(thread, 0L, interval, TimeUnit.SECONDS));
            this.threadPoolExecutor = scheduledthreadPoolExecutor;
            logger.info("scheduled river instance at fixed rate of {} seconds", interval);
        } else {
            this.threadPoolExecutor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<Runnable>());
            futures.add(threadPoolExecutor.submit(thread));
            logger.info("started river instance for single run");
        }
        return futures;
    }

    @Override
    public RiverState getRiverState() {
        GetRiverStateRequestBuilder riverStateRequestBuilder = new GetRiverStateRequestBuilder(client.admin().cluster())
                .setRiverName(riverName.getName())
                .setRiverType(riverName.getType());
        GetRiverStateResponse riverStateResponse = riverStateRequestBuilder.execute().actionGet();
        return riverStateResponse.getRiverState();
    }

    private IngestFactory createIngestFactory(final Settings settings) {
        return new IngestFactory() {
            @Override
            public Ingest create() {
                Integer maxbulkactions = settings.getAsInt("max_bulk_actions", 10000);
                Integer maxconcurrentbulkrequests = settings.getAsInt("max_concurrent_bulk_requests",
                        Runtime.getRuntime().availableProcessors() * 2);
                ByteSizeValue maxvolume = settings.getAsBytesSize("max_bulk_volume", ByteSizeValue.parseBytesSizeValue("10m"));
                TimeValue flushinterval = settings.getAsTime("flush_interval", TimeValue.timeValueSeconds(5));
                return new BulkNodeClient()
                        .maxActionsPerBulkRequest(maxbulkactions)
                        .maxConcurrentBulkRequests(maxconcurrentbulkrequests)
                        .maxVolumePerBulkRequest(maxvolume)
                        .flushIngestInterval(flushinterval)
                        .newClient(client);
            }
        };
    }
}
