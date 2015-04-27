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
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.joda.time.format.DateTimeFormat;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.xbib.elasticsearch.jdbc.strategy.Context;
import org.xbib.elasticsearch.common.util.StrategyLoader;
import org.xbib.elasticsearch.support.client.Ingest;
import org.xbib.elasticsearch.support.client.IngestFactory;
import org.xbib.elasticsearch.support.client.transport.BulkTransportClient;
import org.xbib.pipeline.Pipeline;
import org.xbib.pipeline.PipelineProvider;

import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.common.collect.Queues.newConcurrentLinkedQueue;

/**
 * Standalone feeder for JDBC
 */
public class JDBCFeeder extends TimewindowFeeder {

    private final static Logger logger = LogManager.getLogger("feeder.jdbc");

    protected Context context;

    protected IngestFactory ingestFactory;

    private final static List<JDBCFeeder> feeders = new LinkedList<>();

    private volatile boolean closed;

    @Override
    protected PipelineProvider<Pipeline> pipelineProvider() {
        return new PipelineProvider<Pipeline>() {
            @Override
            public Pipeline get() {
                JDBCFeeder feeder = new JDBCFeeder();
                feeders.add(feeder);
                return feeder;
            }
        };
    }

    @Override
    protected void prepare() throws IOException {
        Runtime.getRuntime().addShutdownHook(shutdownHook());
        ingest = createIngest();
        String timeWindow = settings.get("timewindow") != null ?
                DateTimeFormat.forPattern(settings.get("timewindow")).print(new DateTime()) : "";
        setConcreteIndex(resolveAlias(settings.get("index") + timeWindow));
        Pattern pattern = Pattern.compile("^(.*?)\\d+$");
        Matcher m = pattern.matcher(getConcreteIndex());
        setIndex(m.matches() ? m.group() : getConcreteIndex());
        logger.info("base index name = {}, concrete index name = {}",
                getIndex(), getConcreteIndex());
        Integer maxbulkactions = settings.getAsInt("maxbulkactions", 1000);
        Integer maxconcurrentbulkrequests = settings.getAsInt("maxconcurrentbulkrequests",
                Runtime.getRuntime().availableProcessors());
        ingest.maxActionsPerBulkRequest(maxbulkactions)
                .maxConcurrentBulkRequests(maxconcurrentbulkrequests);
        createIndex(getConcreteIndex());
        input = newConcurrentLinkedQueue();
        input.offer(settings);
    }

    @Override
    protected List<Future> schedule(Settings settings) {
        settings = settings.getAsSettings("jdbc");
        return super.schedule(settings);
    }

    @Override
    protected void process(int counter, Settings settings) throws Exception {
        settings = settings.getAsSettings("jdbc");
        String strategy = settings.get("strategy", "standard");
        this.context = StrategyLoader.newContext(strategy);
        logger.info("strategy {}: settings = {}, context = {}",
                strategy, settings.getAsMap(), context);
        this.ingestFactory = createIngestFactory(settings);
        // simple execution in sync
        context.setCounter(getCounter())
                .setSettings(settings)
                .setIngestFactory(ingestFactory)
                .execute();
    }

    public Context getContext() {
        return context;
    }

    public Set<Context.State> getStates() {
        Set<Context.State> states = new HashSet<>();
        for (JDBCFeeder feeder : feeders) {
            if (feeder.getContext() != null) {
                states.add(feeder.getContext().getState());
            }
        }
        return states;
    }

    public boolean isIdle() {
        Set<Context.State> states = getStates();
        return states.contains(Context.State.IDLE) && states.size() == 1;
    }

    public boolean isActive() {
        Set<Context.State> states = getStates();
        return states.contains(Context.State.FETCH)
                || states.contains(Context.State.BEFORE_FETCH)
                || states.contains(Context.State.AFTER_FETCH);
    }

    public boolean waitFor(Context.State state, long millis) throws InterruptedException {
        long t0 = System.currentTimeMillis();
        boolean found;
        do {
            Set<Context.State> states = getStates();
            found = states.contains(state);
            if (!found) {
                Thread.sleep(100L);
            }
        } while (!found && System.currentTimeMillis() - t0 < millis);
        return found;
    }

    public Thread shutdownHook() {
        return new Thread() {
            public void run() {
                try {
                    shutdown();
                } catch (Exception e) {
                    // logger may already be gc'ed
                    e.printStackTrace();
                }
            }
        };
    }

    public synchronized void shutdown() throws Exception {
        if (closed) {
            return;
        }
        closed = true;
        for (JDBCFeeder feeder : feeders) {
            feeder.shutdown();
        }
        super.shutdown();
        if (context != null) {
            context.getSource().shutdown();
            context.getSink().shutdown();
        }
        if (ingest != null) {
            ingest.shutdown();
        }
    }

    private IngestFactory createIngestFactory(final Settings settings) {
        logger.info("createIngestFactory settings={}", settings.getAsStructuredMap());
        return new IngestFactory() {
            @Override
            public Ingest create() throws IOException {
                Integer maxbulkactions = settings.getAsInt("max_bulk_actions", 10000);
                Integer maxconcurrentbulkrequests = settings.getAsInt("max_concurrent_bulk_requests",
                        Runtime.getRuntime().availableProcessors() * 2);
                ByteSizeValue maxvolume = settings.getAsBytesSize("max_bulk_volume", ByteSizeValue.parseBytesSizeValue("10m"));
                TimeValue flushinterval = settings.getAsTime("flush_interval", TimeValue.timeValueSeconds(5));
                BulkTransportClient ingest = new BulkTransportClient();
                Settings clientSettings = ImmutableSettings.settingsBuilder()
                        .put("cluster.name", settings.get("elasticsearch.cluster", "elasticsearch"))
                        .putArray("host", settings.getAsArray("elasticsearch.host"))
                        .put("port", settings.getAsInt("elasticsearch.port", 9300))
                        .put("sniff", settings.getAsBoolean("elasticsearch.sniff", false))
                        .put("autodiscover", settings.getAsBoolean("elasticsearch.autodiscover", false))
                        .put("name", "feeder") // prevents lookup of names.txt, we don't have it, and marks this node as "feeder"
                        .put("client.transport.ignore_cluster_name", false) // ignore cluster name setting
                        .put("client.transport.ping_timeout", settings.getAsTime("elasticsearch.timeout", TimeValue.timeValueSeconds(5))) //  ping timeout
                        .put("client.transport.nodes_sampler_interval", settings.getAsTime("elasticsearch.timeout", TimeValue.timeValueSeconds(5))) // for sniff sampling
                        .build();
                ingest.maxActionsPerBulkRequest(maxbulkactions)
                        .maxConcurrentBulkRequests(maxconcurrentbulkrequests)
                        .maxVolumePerBulkRequest(maxvolume)
                        .flushIngestInterval(flushinterval)
                        .newClient(clientSettings);
                return ingest;
            }
        };
    }
}
