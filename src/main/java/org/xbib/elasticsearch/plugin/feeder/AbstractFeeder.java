package org.xbib.elasticsearch.plugin.feeder;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.loader.JsonSettingsLoader;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.xbib.cron.CronExpression;
import org.xbib.cron.CronThreadPoolExecutor;
import org.xbib.elasticsearch.action.river.jdbc.state.RiverState;
import org.xbib.elasticsearch.support.client.Ingest;
import org.xbib.elasticsearch.support.client.node.NodeClient;
import org.xbib.io.URIUtil;
import org.xbib.pipeline.AbstractPipeline;
import org.xbib.pipeline.Pipeline;
import org.xbib.pipeline.PipelineException;
import org.xbib.pipeline.PipelineProvider;
import org.xbib.pipeline.PipelineRequest;
import org.xbib.pipeline.element.MapPipelineElement;
import org.xbib.pipeline.simple.SimplePipelineExecutor;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.collect.Lists.newLinkedList;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Base class for all feeders and feed pipelines.
 * A feed pipeline is a sequence of URI in form of pipeline elements that can be consumed
 * in serial or parallel manner.
 * Variables - mostly read-only - that are common to all pipeline executions are declared
 * as static variables.
 *
 * @param <T> the pipeline element
 * @param <R> the pipeline request
 * @param <P> the pipeline exception
 */
public abstract class AbstractFeeder<T, R extends PipelineRequest, P extends Pipeline<T, R>>
        extends AbstractPipeline<MapPipelineElement, PipelineException>
        implements Feeder<T, R, P> {

    private final static ESLogger logger = ESLoggerFactory.getLogger(Feeder.class.getSimpleName());

    /**
     * The feeder type
     */
    private String type;

    /**
     * The specification for the feeder
     */
    protected Map<String, Object> spec;

    /**
     * The settings for the feeder, derived from the specification
     */
    protected Settings settings;

    /**
     * The sources for the feed processing
     */
    protected Queue<Map<String, Object>> queue;

    /**
     * The ingester routine, shared by all pipelines.
     */
    protected Ingest ingest;

    /**
     * The state of the feeder, relevant for persistency if the feeder is running as a river
     */
    protected RiverState riverState;

    /**
     * This executor can start the feeder at scheduled times.
     */
    private ScheduledThreadPoolExecutor scheduledThreadPoolExecutor;

    /**
     * The executor for pipelines.
     */
    private SimplePipelineExecutor<T, R, P> executor;

    /**
     * A flag that indicates the feeder should terminate.
     */
    private volatile boolean interrupted;

    private List<AbstractFeeder> registry = newLinkedList();

    public AbstractFeeder() {
        registry.add(this);
    }

    @SuppressWarnings({"unchecked"})
    public AbstractFeeder(AbstractFeeder feeder) {
        this.type = feeder.type;
        this.spec = feeder.spec;
        this.settings = feeder.settings;
        this.queue = feeder.queue;
        this.ingest = feeder.ingest;
        this.scheduledThreadPoolExecutor = feeder.scheduledThreadPoolExecutor;
        this.riverState = feeder.riverState;
        feeder.registry.add(this);
    }

    @Override
    public Feeder<T, R, P> readFrom(Reader reader) {
        try {
            Map<String, Object> spec = XContentFactory.xContent(XContentType.JSON).createParser(reader).mapOrderedAndClose();
            Map<String, String> loadedSettings = new JsonSettingsLoader().load(jsonBuilder().map(spec).string());
            Settings settings = settingsBuilder().put(loadedSettings).build();
            setSpec(spec);
            setSettings(settings);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
        return this;
    }

    @Override
    public Feeder<T, R, P> setType(String type) {
        this.type = type;
        return this;
    }

    public String getType() {
        return type;
    }

    @Override
    public Feeder<T, R, P> setSpec(Map<String, Object> spec) {
        this.spec = spec;
        return this;
    }

    @Override
    public Map<String, Object> getSpec() {
        return spec;
    }


    @Override
    public Feeder<T, R, P> setSettings(Settings newSettings) {
        this.settings = newSettings;
        return this;
    }

    @Override
    public Settings getSettings() {
        return settings;
    }

    @Override
    public void schedule(Thread thread) {
        if (settings == null) {
            throw new IllegalArgumentException("no settings?");
        }
        String[] schedule = settings.getAsArray("schedule");
        Long seconds = settings.getAsTime("interval", TimeValue.timeValueSeconds(0)).seconds();
        if (schedule != null && schedule.length > 0) {
            CronThreadPoolExecutor e = new CronThreadPoolExecutor(settings.getAsInt("cronpoolsize", 4));
            for (String cron : schedule) {
                e.schedule(thread, new CronExpression(cron));
            }
            this.scheduledThreadPoolExecutor = e;
            logger.info("scheduled feeder with cron expressions {}", Arrays.asList(schedule));
        } else if (seconds > 0L) {
            this.scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(settings.getAsInt("cronpoolsize", 4));
            scheduledThreadPoolExecutor.scheduleAtFixedRate(thread, 0L, seconds, TimeUnit.SECONDS);
            logger.info("scheduled feeder at fixed rate of {} seconds", seconds);
        } else {
            thread.start();
            logger.info("started feeder thread");
        }
    }

    @Override
    public Feeder<T, R, P> setClient(Client client) {
        if (settings == null) {
            throw new IllegalArgumentException("no settings?");
        }
        Integer maxbulkactions = settings.getAsInt("maxbulkactions", 100);
        Integer maxconcurrentbulkrequests = settings.getAsInt("maxconcurrentbulkrequests",
                Runtime.getRuntime().availableProcessors());
        ByteSizeValue maxvolume = settings.getAsBytesSize("maxbulkvolume", ByteSizeValue.parseBytesSizeValue("10m"));
        TimeValue maxrequestwait = settings.getAsTime("maxrequestwait", TimeValue.timeValueSeconds(60));
        this.ingest = new NodeClient()
                .maxActionsPerBulkRequest(maxbulkactions)
                .maxConcurrentBulkRequests(maxconcurrentbulkrequests)
                .maxRequestWait(maxrequestwait)
                .maxVolumePerBulkRequest(ByteSizeValue.parseBytesSizeValue(maxvolume.toString()))
                .newClient(client);
        return this;
    }

    @Override
    public Client getClient() {
        return ingest != null ? ingest.client() : null;
    }

    @Override
    public Feeder<T, R, P> setRiverState(RiverState riverState) {
        this.riverState = riverState;
        return this;
    }

    @Override
    public RiverState getRiverState() {
        return riverState;
    }

    @Override
    public void run() {
        if (settings == null) {
            throw new IllegalArgumentException("no settings?");
        }
        logger.trace("starting run with settings {}", settings.getAsMap());
        try {
            beforeRun();
            this.executor = new SimplePipelineExecutor<T, R, P>()
                    .setConcurrency(settings.getAsInt("concurrency", 1))
                    .setPipelineProvider(pipelineProvider())
                    .prepare()
                    .execute()
                    .waitFor();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("executor interrupted");
        } catch (Throwable t) {
            logger.error(t.getMessage(), t);
        } finally {
            logger.trace("run completed with settings {}", settings.getAsMap());
            try {
                afterRun();
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
        }
    }

    @Override
    @SuppressWarnings({"unchecked"})
    public Feeder<T, R, P> beforeRun() throws IOException {
        // build input queue
        this.queue = new ConcurrentLinkedQueue<Map<String, Object>>();
        Object input = getType() != null && spec.containsKey(getType()) ? spec.get(getType()) : spec;
        if (input instanceof Object[]) {
            for (Object map : (Object[]) input) {
                if (map instanceof Map) {
                    queue.offer((Map<String, Object>) map);
                } else {
                    logger.warn("'input' field does not contain a map, skipping: {}", map);
                }
            }
        } else if (input instanceof List) {
            for (Object map : (List) input) {
                if (map instanceof Map) {
                    queue.offer((Map<String, Object>) map);
                } else {
                    logger.warn("'input' field does not contain a map, skipping: {}", map);
                }
            }
        } else if (input instanceof Map) {
            queue.offer((Map<String, Object>) input);
        }
        return this;
    }

    @Override
    public Feeder<T, R, P> afterRun() throws IOException {
        try {
            if (executor != null) {
                logger.info("shutting down executor");
                executor.shutdown();
                executor = null;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("executor shutdown interrupted");
        }
        if (ingest != null) {
            ingest.flushIngest();
        }
        return this;
    }

    /**
     * Interrupt this feeder. The feeder pipelines will terminate as soon as possible.
     *
     * @param state true if the feeder should be interrupted
     */
    @Override
    public void setInterrupted(boolean state) {
        interrupted = state;
        for (Feeder feeder : registry) {
            if (feeder != this) {
                feeder.setInterrupted(state);
            }
        }
    }

    @Override
    public boolean isInterrupted() {
        return interrupted;
    }

    /**
     * Closing down a feeder thread. Normally, a no-op.
     *
     * @throws java.io.IOException
     */
    @Override
    public void close() throws IOException {
        logger.info("close (no-op)");
    }

    /**
     * Finish up this feeder. After calling shutdown, no more feeder threads or
     * methods of this instance may work correctly because they are no longer available for service.
     */
    @Override
    public void shutdown() {
        logger.info("shutting down ...");
        if (scheduledThreadPoolExecutor != null) {
            logger.info("shutting down scheduler");
            scheduledThreadPoolExecutor.shutdownNow();
            scheduledThreadPoolExecutor = null;
        }
        logger.info("interrupting...");
        setInterrupted(true);
    }

    @Override
    public boolean hasNext() {
        return !queue.isEmpty() && !interrupted;
    }

    @Override
    public MapPipelineElement next() {
        return new MapPipelineElement().set(queue.poll());
    }

    @Override
    public void newRequest(Pipeline<Boolean, MapPipelineElement> pipeline, MapPipelineElement request) {
        try {
            executeTask(request.get());
        } catch (Exception ex) {
            logger.error("error while getting next input: " + ex.getMessage(), ex);
        }
    }

    @Override
    public void error(Pipeline<Boolean, MapPipelineElement> pipeline, MapPipelineElement request, PipelineException error) {
        logger.error(error.getMessage(), error);
    }

    @Override
    public Thread shutdownHook() {
        return new Thread() {
            public void run() {
                for (AbstractFeeder feeder : registry) {
                    try {
                        if (feeder.scheduledThreadPoolExecutor != null) {
                            logger.info("shutting down scheduler");
                            feeder.scheduledThreadPoolExecutor.shutdownNow();
                            feeder.scheduledThreadPoolExecutor = null;
                        }
                        if (feeder.executor != null) {
                            logger.info("shutting down executor");
                            feeder.executor.shutdown();
                        }
                    } catch (InterruptedException e) {
                        logger.warn("interrupted", e);
                    } catch (IOException e) {
                        logger.error(e.getMessage(), e);
                    }
                }
                if (ingest != null) {
                    logger.info("shutting down ingester");
                    ingest.shutdown();
                }
                logger.info("shutdown completed");
            }
        };
    }

    public InputStream getDefaultSettings(String index) {
        return AbstractFeeder.class.getResourceAsStream("/" + index + "/settings");
    }

    public InputStream getDefaultMapping(String index, String type) {
        return AbstractFeeder.class.getResourceAsStream("/" + index + "/" + type + ".mapping");
    }

    public abstract PipelineProvider<P> pipelineProvider();

    public abstract void executeTask(Map<String, Object> map) throws Exception;

    public Settings clientSettings(URI connectionSpec) {
        return settingsBuilder()
                .put("name", "feeder") // prevents lookup of names.txt, we don't have it, and marks this node as "feeder". See also module load skipping in JDBCRiverPlugin
                .put("network.server", false) // this is not a server
                .put("node.client", true) // this is an Elasticearch client
                .put("cluster.name", URIUtil.parseQueryString(connectionSpec).get("es.cluster.name")) // specified remote ES cluster
                .put("client.transport.sniff", false) // we do not sniff (should be configurable)
                .put("client.transport.ignore_cluster_name", false) // respect cluster name setting
                .put("client.transport.ping_timeout", "30s") // large ping timeout (should not be required)
                .put("client.transport.nodes_sampler_interval", "30s") // only for sniff sampling
                .put("path.plugins", ".dontexist") // pointing to a non-exiting folder means, this disables loading site plugins
                // we do not need to change class path settings when using the "feeder" name trick
                .build();
    }

}
