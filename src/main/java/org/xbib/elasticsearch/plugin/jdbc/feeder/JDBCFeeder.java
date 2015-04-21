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
package org.xbib.elasticsearch.plugin.jdbc.feeder;

import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.metrics.MeterMetric;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.loader.JsonSettingsLoader;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.river.RiverName;
import org.xbib.elasticsearch.plugin.jdbc.RiverRunnable;
import org.xbib.elasticsearch.plugin.jdbc.classloader.uri.URIClassLoader;
import org.xbib.elasticsearch.plugin.jdbc.client.Ingest;
import org.xbib.elasticsearch.plugin.jdbc.client.IngestFactory;
import org.xbib.elasticsearch.plugin.jdbc.client.transport.BulkTransportClient;
import org.xbib.elasticsearch.plugin.jdbc.cron.CronExpression;
import org.xbib.elasticsearch.plugin.jdbc.cron.CronThreadPoolExecutor;
import org.xbib.elasticsearch.plugin.jdbc.state.RiverStatesMetaData;
import org.xbib.elasticsearch.plugin.jdbc.util.RiverServiceLoader;
import org.xbib.elasticsearch.river.jdbc.RiverFlow;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.Reader;
import java.io.Writer;
import java.security.Security;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
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
 * Standalone feeder for JDBC
 */
public class JDBCFeeder {

    private final static ESLogger logger = ESLoggerFactory.getLogger("JDBCFeeder");

    /**
     * Register metadata factory in Elasticsearch for being able to decode
     * ClusterStateResponse with RiverStatesMetadata
     */
    static {
        MetaData.registerFactory(RiverStatesMetaData.TYPE, RiverStatesMetaData.FACTORY);
    }

    protected Reader reader;

    protected Writer writer;

    protected PrintStream printStream;

    protected IngestFactory ingestFactory;

    /**
     * This ingest is the client for the river flow state operations
     */
    private Ingest ingest;

    private RiverFlow riverFlow;

    private List<Map<String, Object>> definitions;

    private ThreadPoolExecutor threadPoolExecutor;

    private volatile Thread feederThread;

    private volatile boolean closed;

    /**
     * Constructor for running this from command line
     */
    public JDBCFeeder() {
        // disable DNS caching for found.no failover
        Security.setProperty("networkaddress.cache.ttl", "0");
        Runtime.getRuntime().addShutdownHook(shutdownHook());
    }

    /**
     * Invoked by Runner
     *
     * @throws Exception
     */
    public void exec() throws Exception {
        readFrom(new InputStreamReader(System.in, "UTF-8"))
                .writeTo(new OutputStreamWriter(System.out, "UTF-8"))
                .errorsTo(System.err)
                .start();
    }

    @SuppressWarnings("unchecked")
    public JDBCFeeder readFrom(Reader reader) {
        this.reader = reader;
        try {
            Map<String, Object> map = XContentFactory.xContent(XContentType.JSON).createParser(reader).mapOrderedAndClose();
            Settings settings = settingsBuilder()
                    .put(new JsonSettingsLoader().load(jsonBuilder().map(map).string()))
                    .build();
            this.definitions = newLinkedList();
            Object pipeline = map.get("jdbc");
            if (pipeline instanceof Map) {
                definitions.add((Map<String, Object>) pipeline);
            }
            if (pipeline instanceof List) {
                definitions.addAll((List<Map<String, Object>>) pipeline);
            }
            // before running, create the river flow
            createRiverFlow(map, settings);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
        return this;
    }

    protected RiverFlow createRiverFlow(Map<String, Object> spec, Settings settings) throws IOException {
        String strategy = XContentMapValues.nodeStringValue(spec.get("strategy"), "simple");
        this.riverFlow = RiverServiceLoader.newRiverFlow(strategy);
        logger.debug("strategy {}: river flow class {}, spec = {} settings = {}",
                strategy, riverFlow.getClass().getName(), spec, settings.getAsMap());
        this.ingestFactory = createIngestFactory(settings);
        // out private ingest, needed for having a client in the river flow
        this.ingest = ingestFactory.create();
        riverFlow.setRiverName(new RiverName("jdbc", "feeder"))
                .setSettings(settings)
                .setClient(ingest.client())
                .setIngestFactory(ingestFactory)
                .setMetric(new MeterMetric(Executors.newScheduledThreadPool(1), TimeUnit.SECONDS))
                .setQueue(new ConcurrentLinkedDeque<Map<String, Object>>());
        return riverFlow;
    }

    public JDBCFeeder writeTo(Writer writer) {
        this.writer = writer;
        return this;
    }

    public JDBCFeeder errorsTo(PrintStream printStream) {
        this.printStream = printStream;
        return this;
    }

    public JDBCFeeder start() throws Exception {
        this.closed = false;
        this.feederThread = new Thread(new RiverRunnable(riverFlow, definitions));
        List<Future<?>> futures = schedule(feederThread);
        // wait for all threads to finish
        for (Future<?> future : futures) {
            future.get();
        }
        ingest.shutdown();
        return this;
    }

    private List<Future<?>> schedule(Thread thread) {
        Settings settings = riverFlow.getSettings();
        String[] schedule = settings.getAsArray("schedule");
        List<Future<?>> futures = newLinkedList();
        Long seconds = settings.getAsTime("interval", TimeValue.timeValueSeconds(0)).seconds();
        if (schedule != null && schedule.length > 0) {
            CronThreadPoolExecutor cronThreadPoolExecutor =
                    new CronThreadPoolExecutor(settings.getAsInt("threadpoolsize", 1));
            for (String cron : schedule) {
                futures.add(cronThreadPoolExecutor.schedule(thread, new CronExpression(cron)));
            }
            this.threadPoolExecutor = cronThreadPoolExecutor;
            logger.debug("scheduled feeder instance with cron expressions {}", Arrays.asList(schedule));
        } else if (seconds > 0L) {
            ScheduledThreadPoolExecutor scheduledThreadPoolExecutor =
                    new ScheduledThreadPoolExecutor(settings.getAsInt("threadpoolsize", 4));
            futures.add(scheduledThreadPoolExecutor.scheduleAtFixedRate(thread, 0L, seconds, TimeUnit.SECONDS));
            logger.debug("scheduled feeder instance at fixed rate of {} seconds", seconds);
            this.threadPoolExecutor = scheduledThreadPoolExecutor;
        } else {
            this.threadPoolExecutor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<Runnable>());
            futures.add(threadPoolExecutor.submit(thread));
            logger.debug("started feeder instance");
        }
        return futures;
    }

    /**
     * Shut down feeder instance by Ctrl-C
     *
     * @return shutdown thread
     */
    public Thread shutdownHook() {
        return new Thread() {
            public void run() {
                try {
                    shutdown();
                } catch (Exception e) {
                    e.printStackTrace(printStream);
                }
            }
        };
    }

    public synchronized void shutdown() throws Exception {
        if (closed) {
            return;
        }
        closed = true;
        if (threadPoolExecutor != null) {
            threadPoolExecutor.shutdownNow();
            threadPoolExecutor = null;
        }
        if (feederThread != null) {
            feederThread.interrupt();
        }
        if (!ingest.isShutdown()) {
            ingest.shutdown();
        }
        reader.close();
        writer.close();
        printStream.close();
    }

    private IngestFactory createIngestFactory(final Settings settings) {
        return new IngestFactory() {
            @Override
            public Ingest create() throws IOException {
                Integer maxbulkactions = settings.getAsInt("max_bulk_actions", 10000);
                Integer maxconcurrentbulkrequests = settings.getAsInt("max_concurrent_bulk_requests",
                        Runtime.getRuntime().availableProcessors() * 2);
                ByteSizeValue maxvolume = settings.getAsBytesSize("max_bulk_volume", ByteSizeValue.parseBytesSizeValue("10m"));
                TimeValue flushinterval = settings.getAsTime("flush_interval", TimeValue.timeValueSeconds(5));
                File home = new File(settings.get("home", "."));
                BulkTransportClient ingest = new BulkTransportClient();
                ImmutableSettings.Builder clientSettings = ImmutableSettings.settingsBuilder()
                        .put("cluster.name", settings.get("elasticsearch.cluster", "elasticsearch"))
                        .putArray("host", settings.getAsArray("elasticsearch.host"))
                        .put("port", settings.getAsInt("elasticsearch.port", 9300))
                        .put("sniff", settings.getAsBoolean("elasticsearch.sniff", false))
                        .put("autodiscover", settings.getAsBoolean("elasticsearch.autodiscover", false))
                        .put("name", "feeder") // prevents lookup of names.txt, we don't have it, and marks this node as "feeder". See also module load skipping in JDBCRiverPlugin
                        .put("client.transport.ignore_cluster_name", false) // do not ignore cluster name setting
                        .put("client.transport.ping_timeout", settings.getAsTime("elasticsearch.timeout", TimeValue.timeValueSeconds(10))) //  ping timeout
                        .put("client.transport.nodes_sampler_interval", settings.getAsTime("elasticsearch.timeout", TimeValue.timeValueSeconds(5))) // for sniff sampling
                        .put("path.plugins", ".dontexist") // pointing to a non-exiting folder means, this disables loading site plugins
                                // adding our custom class loader is tricky, actions may not be registered to ActionService
                        .classLoader(getClassLoader(getClass().getClassLoader(), home));
                                // optional found.no transport plugin
                if (settings.get("transport.type") != null) {
                    clientSettings.put("transport.type", settings.get("transport.type"));
                }
                // copy found.no transport settings
                Settings foundTransportSettings = settings.getAsSettings("transport.found");
                if (foundTransportSettings != null) {
                    ImmutableMap<String,String> foundTransportSettingsMap = foundTransportSettings.getAsMap();
                    for (Map.Entry<String,String> entry : foundTransportSettingsMap.entrySet()) {
                        clientSettings.put("transport.found." + entry.getKey(), entry.getValue());
                    }
                }
                ingest.maxActionsPerBulkRequest(maxbulkactions)
                        .maxConcurrentBulkRequests(maxconcurrentbulkrequests)
                        .maxVolumePerBulkRequest(maxvolume)
                        .flushIngestInterval(flushinterval)
                        .newClient(clientSettings.build());
                return ingest;
            }
        };
    }

    /**
     * We have to add Elasticsearch to our classpath, but exclude all jvm plugins
     * for starting our TransportClient.
     *
     * @param home ES_HOME
     * @return a custom class loader with our dependencies
     */
    private ClassLoader getClassLoader(ClassLoader parent, File home) {
        URIClassLoader classLoader = new URIClassLoader(parent);
        File[] libs = new File(home + "/lib").listFiles();
        if (libs != null) {
            for (File file : libs) {
                if (file.getName().toLowerCase().endsWith(".jar")) {
                    classLoader.addURI(file.toURI());
                }
            }
        }
        return classLoader;
    }

}
