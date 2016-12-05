package org.xbib.importer;

import org.xbib.content.settings.Settings;
import org.xbib.content.util.unit.TimeValue;
import org.xbib.importer.util.CronExpression;
import org.xbib.importer.util.concurrent.ExceptionService;
import org.xbib.importer.util.concurrent.ExtendedCronThreadPoolExecutor;
import org.xbib.importer.util.concurrent.ExtendedScheduledThreadPoolExecutor;
import org.xbib.importer.util.concurrent.ExtendedThreadPoolExecutor;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 */
public class Importer extends AbstractWorkerPool<Settings> implements WorkerPool<Settings> {

    private static final Logger logger = Logger.getLogger(Importer.class.getName());

    private Settings settings;

    private String prefix;

    private Sink sink;

    private ImporterListener listener;

    private List<Worker<Settings>> workers;

    private List<Future<?>> futures;

    public Importer(Settings settings, String prefix) {
        this(settings, prefix, settings.getAsInt("concurrency", Runtime.getRuntime().availableProcessors()));
    }

    public Importer(Settings settings, String prefix, int workerCount) {
        this(settings, prefix, workerCount, new DefaultExceptionService(), new DefaultImporterListener());
    }

    public Importer(Settings settings, String prefix, int workerCount,
                    ExceptionService exceptionService, ImporterListener listener) {
        super(workerCount, createThreadPoolExecutor(settings, exceptionService), exceptionService);
        this.settings = settings;
        this.prefix = prefix;
        this.listener = listener;
        this.futures = new ArrayList<>();
        this.workers = new ArrayList<>();
    }

    @Override
    public Importer open() {
        // find requests and submit to queue
        logger.log(Level.INFO, "opening, settings=" + settings.getAsMap());
        // create sink
        this.sink = createSink(settings);
        // spawn wrapped workers
        super.open();
        Map<String, Settings> map = settings.getGroups("input." + prefix);
        for (Settings request : map.values()) {
            logger.log(Level.INFO, "submitting " + request.getAsMap());
            submit(request);
        }
        return this;
    }

    @Override
    public Settings getPoison() {
        return Settings.EMPTY_SETTINGS;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Worker<Settings> newWorker() {
        try {
            Class<?> clazz = Class.forName(settings.get("program.worker",
                    "org.xbib.importer." + prefix + ".ImporterWorker"));
            Class<?>[] parameterClasses = new Class[3];
            parameterClasses[0] = Settings.class;
            parameterClasses[1] = Sink.class;
            parameterClasses[2] = ImporterListener.class;
            Constructor<?> constructor = clazz.getConstructor(parameterClasses);
            Worker<Settings> worker = (Worker<Settings>) constructor.newInstance(settings, sink, listener);
            workers.add(worker);
            schedule(settings, wrap(worker));
            return worker;
        } catch (ClassNotFoundException | NoSuchMethodException |
                IllegalAccessException | InvocationTargetException | InstantiationException e) {
            logger.log(Level.SEVERE, e.getMessage(), e);
        }
        return null;
    }

    @Override
    public void close() {
        super.close();
        try {
            if (sink != null) {
                sink.close();
            }
        } catch (IOException e) {
            logger.log(Level.SEVERE, e.getMessage(), e);
        }
    }

    private void schedule(Settings settings, Runnable runnable) {
        logger.log(Level.INFO, "scheduling, settings " + settings.getAsMap());
        Long seconds = settings.getAsTime("interval", TimeValue.timeValueSeconds(0)).seconds();
        String[] schedule = settings.getAsArray("schedule");
        if (schedule != null && schedule.length > 0) {
            logger.log(Level.INFO, "scheduling " + runnable);
            ExtendedCronThreadPoolExecutor cronThreadPoolExecutor =
                    (ExtendedCronThreadPoolExecutor) getExecutor();
            for (String cron : schedule) {
                futures.add(cronThreadPoolExecutor.schedule(runnable, new CronExpression(cron)));
            }
            logger.log(Level.INFO, "scheduled " + runnable + " with cron expressions " + Arrays.asList(schedule));
        } else if (seconds > 0L) {
            logger.log(Level.INFO, "interval run of " + runnable);
            ExtendedScheduledThreadPoolExecutor scheduledThreadPoolExecutor =
                    (ExtendedScheduledThreadPoolExecutor)getExecutor();
            futures.add(scheduledThreadPoolExecutor.scheduleAtFixedRate(runnable, 0L, seconds, TimeUnit.SECONDS));
            logger.log(Level.INFO, "interval running " + runnable + " at fixed rate of " + seconds + " seconds");
        } else {
            logger.log(Level.INFO, "one time run of " + runnable);
            ExtendedThreadPoolExecutor extendedThreadPoolExecutor =
                    (ExtendedThreadPoolExecutor)getExecutor();
            futures.add(extendedThreadPoolExecutor.submit(runnable));
            logger.log(Level.INFO, "scheduled " + runnable + " for one-time run");
        }
    }

    private static ThreadPoolExecutor createThreadPoolExecutor(Settings settings, ExceptionService exceptionService) {
        Long seconds = settings.getAsTime("interval", TimeValue.timeValueSeconds(0)).seconds();
        String[] schedule = settings.getAsArray("schedule");
        if (schedule != null && schedule.length > 0) {
            return new ExtendedCronThreadPoolExecutor(settings.getAsInt("threadpoolsize", 1),
                    exceptionService);
        } else if (seconds > 0L) {
            return new ExtendedScheduledThreadPoolExecutor(settings.getAsInt("threadpoolsize", 1),
                    exceptionService);
        } else {
            return new ExtendedThreadPoolExecutor(1, exceptionService);
        }
    }

    private Sink createSink(Settings settings) {
        // naive approach: take the first output found and create a sink
        Map<String, Settings> map = settings.getGroups("output");
        logger.log(Level.INFO, "outputs=" + map.keySet());
        for (Map.Entry<String, Settings> entry : map.entrySet()) {
            if (entry.getValue().getAsBoolean("enabled", true)) {
                try {
                    String key = entry.getKey();
                    logger.log(Level.INFO, "trying to create sink from key=" + key);
                    String name = key.substring(0, 1).toUpperCase() + key.substring(1);
                    Class<?> clazz = Class.forName("org.xbib.importer." + key + "." + name + "Sink");
                    Class<?>[] parameterClasses = new Class[2];
                    parameterClasses[0] = Settings.class;
                    parameterClasses[1] = ImporterListener.class;
                    Constructor<?> constructor = clazz.getConstructor(parameterClasses);
                    return (Sink) constructor.newInstance(entry.getValue(), listener);
                } catch (Throwable e) {
                    logger.log(Level.SEVERE, e.getMessage(), e);
                }
            } else {
                logger.log(Level.WARNING, entry.getKey() + " is not enabled");
            }
        }
        throw new IllegalArgumentException("no sink found, output=" + map.keySet());
    }
}
