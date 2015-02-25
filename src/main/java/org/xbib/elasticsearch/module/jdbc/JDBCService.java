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
package org.xbib.elasticsearch.module.jdbc;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.metrics.MeterMetric;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.loader.JsonSettingsLoader;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.xbib.elasticsearch.common.client.Ingest;
import org.xbib.elasticsearch.common.client.IngestFactory;
import org.xbib.elasticsearch.common.client.node.BulkNodeClient;
import org.xbib.elasticsearch.common.cron.CronExpression;
import org.xbib.elasticsearch.common.cron.CronThreadPoolExecutor;
import org.xbib.elasticsearch.jdbc.feeder.Feeder;
import org.xbib.elasticsearch.jdbc.strategy.Flow;
import org.xbib.elasticsearch.common.util.StrategyLoader;

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
import static org.elasticsearch.common.collect.Maps.newHashMap;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class JDBCService extends AbstractLifecycleComponent<JDBCService> {

    private final Client client;

    private final Map<String,JDBCTask> tasks = newHashMap();

    private String name;

    private Flow flow;

    private List<Map<String, Object>> definitions;

    private ThreadPoolExecutor threadPoolExecutor;

    private volatile Thread thread;

    private volatile boolean closed;

    private String[] schedule;

    private List<Future<?>> futures;

    private Long interval;

    @Inject
    public JDBCService(Settings settings, Client client) {
        super(settings);
        this.client = client;
    }

    @Override
    protected void doStart() throws ElasticsearchException {

    }

    @Override
    protected void doStop() throws ElasticsearchException {

    }

    @Override
    protected void doClose() throws ElasticsearchException {

    }

    public void init() {
        if (settings.get("jdbc") == null) {
            throw new IllegalArgumentException("no 'jdbc' in settings?");
        }
        try {
            this.name = settings.get("name");
            String strategy = XContentMapValues.nodeStringValue(settings.get("strategy"), "simple");
            this.schedule = settings.getAsArray("schedule");
            this.interval = settings.getAsTime("interval", TimeValue.timeValueSeconds(0)).seconds();
            this.definitions = newLinkedList();
            Object definition = settings.getAsStructuredMap().get("jdbc");
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
            this.flow = StrategyLoader.newFlow(strategy);
            logger.debug("strategy {}: flow class {} found, settings = {}",
                    strategy, flow.getClass().getName(), settings.getAsMap());
            flow.setName(settings.get("name"))
                    .setSettings(settings)
                    .setClient(client)
                    .setIngestFactory(createIngestFactory(settings))
                    .setMetric(new MeterMetric(Executors.newScheduledThreadPool(1), TimeUnit.SECONDS))
                    .setQueue(new ConcurrentLinkedQueue<Map<String, Object>>());
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }

    }

    public void startTask() {
        closed = false;
        this.thread = EsExecutors.daemonThreadFactory(settings,
                "task(" + name + ")")
                .newThread(new Feeder(flow, definitions));
        this.futures = schedule(thread);
        // we do not wait for futures here, instead, we return to Elasticsea
    }

    /**
     * Called from Elasticsearch when service is deleted.
     */
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        if (threadPoolExecutor != null) {
            logger.debug("shutting down thread scheduler");
            threadPoolExecutor.shutdownNow();
            threadPoolExecutor = null;
        }
        if (thread != null) {
            logger.debug("interrupting thread");
            thread.interrupt();
        }
        logger.info("plugin closed [{}]", name);
    }

    public void run() {
        if (!closed) {
            close();
        }
        this.thread = EsExecutors.daemonThreadFactory(settings,
                "task(" + name + ")")
                .newThread(new Feeder(flow, definitions));
        this.threadPoolExecutor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>());
        futures.add(threadPoolExecutor.submit(thread));
        logger.info("started task for single run");
    }

    private List<Future<?>> schedule(Thread thread) {
        Settings settings = flow.getSettings();
        List<Future<?>> futures = newLinkedList();
        if (schedule != null && schedule.length > 0) {
            CronThreadPoolExecutor cronThreadPoolExecutor =
                    new CronThreadPoolExecutor(settings.getAsInt("threadpoolsize", 1));
            for (String cron : schedule) {
                futures.add(cronThreadPoolExecutor.schedule(thread, new CronExpression(cron)));
            }
            this.threadPoolExecutor = cronThreadPoolExecutor;
            logger.info("scheduled tasks with cron expressions {}", Arrays.asList(schedule));
        } else if (interval > 0L) {
            ScheduledThreadPoolExecutor scheduledthreadPoolExecutor = new ScheduledThreadPoolExecutor(settings.getAsInt("threadpoolsize", 4));
            futures.add(scheduledthreadPoolExecutor.scheduleAtFixedRate(thread, 0L, interval, TimeUnit.SECONDS));
            this.threadPoolExecutor = scheduledthreadPoolExecutor;
            logger.info("scheduled tasks at fixed rate of {} seconds", interval);
        } else {
            this.threadPoolExecutor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<Runnable>());
            futures.add(threadPoolExecutor.submit(thread));
            logger.info("started task for single run");
        }
        return futures;
    }

    private IngestFactory createIngestFactory(final Settings settings) {
        return new IngestFactory() {
            @Override
            public Ingest create() {
                Integer maxbulkactions = settings.getAsInt("max_bulk_actions", 10000);
                Integer maxconcurrentbulkrequests = settings.getAsInt("max_concurrent_bulk_requests",
                        Runtime.getRuntime().availableProcessors() * 2);
                ByteSizeValue maxvolume = settings.getAsBytesSize("max_bulk_volume", ByteSizeValue.parseBytesSizeValue("10m"));
                TimeValue maxrequestwait = settings.getAsTime("max_request_wait", TimeValue.timeValueSeconds(60));
                TimeValue flushinterval = settings.getAsTime("flush_interval", TimeValue.timeValueSeconds(5));
                return new BulkNodeClient()
                        .maxActionsPerBulkRequest(maxbulkactions)
                        .maxConcurrentBulkRequests(maxconcurrentbulkrequests)
                        .maxRequestWait(maxrequestwait)
                        .maxVolumePerBulkRequest(maxvolume)
                        .flushIngestInterval(flushinterval)
                        .newClient(client);
            }
        };
    }
}
