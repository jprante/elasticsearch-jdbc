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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.xbib.elasticsearch.common.cron.CronExpression;
import org.xbib.elasticsearch.common.cron.CronThreadPoolExecutor;
import org.xbib.elasticsearch.common.util.StrategyLoader;
import org.xbib.elasticsearch.jdbc.strategy.Context;
import org.xbib.pipeline.AbstractPipeline;
import org.xbib.pipeline.Pipeline;
import org.xbib.pipeline.PipelineProvider;
import org.xbib.pipeline.SimplePipelineExecutor;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.settings.Settings.settingsBuilder;

public class JDBCImporter
        extends AbstractPipeline<SettingsPipelineRequest>
        implements Runnable, CommandLineInterpreter {

    private final static Logger logger = LogManager.getLogger("importer.jdbc");

    private Context context;

    private volatile boolean shutdown;

    private static Settings settings = Settings.EMPTY;

    private ExecutorService executorService;

    private ThreadPoolExecutor threadPoolExecutor;

    private List<Future> futures;

    protected PipelineProvider<Pipeline<SettingsPipelineRequest>> pipelineProvider() {
        return new PipelineProvider<Pipeline<SettingsPipelineRequest>>() {
            @Override
            public Pipeline<SettingsPipelineRequest> get() {
                JDBCImporter jdbcImporter = new JDBCImporter();
                jdbcImporter.setQueue(getQueue());
                return jdbcImporter;
            }
        };
    }

    public JDBCImporter setSettings(Settings newSettings) {
        logger.debug("settings = {}", newSettings.getAsMap());
        settings = newSettings;
        String statefile = settings.get("jdbc.statefile");
        if (statefile != null) {
            try {
                File file = new File(statefile);
                if (file.exists() && file.isFile() && file.canRead()) {
                    InputStream stateFileInputStream = new FileInputStream(file);
                    settings = settingsBuilder().put(settings).loadFromStream("statefile", stateFileInputStream).build();
                    logger.info("loaded state from {}, settings {}", statefile, settings.getAsMap());
                } else {
                    logger.warn("can't read from {}, skipped", statefile);
                }
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
        }
        return this;
    }

    public JDBCImporter reloadSettings(Settings oldSettings) {
        String statefile = oldSettings.get("statefile");
        if (statefile != null) {
            try {
                File file = new File(statefile);
                if (file.exists() && file.isFile() && file.canRead()) {
                    InputStream stateFileInputStream = new FileInputStream(file);
                    settings = settingsBuilder().put(oldSettings).loadFromStream("statefile", stateFileInputStream).build();
                    logger.info("reloaded state from {}, settings {} ", statefile, settings.getAsMap());
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
    public void run(String resourceName, InputStream in) {
        setSettings(settingsBuilder().loadFromStream(resourceName, in).build());
        run();
    }

    @Override
    public void run() {
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
                        logger.error(e.getMessage());
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
                if (context != null) {
                    context.shutdown();
                    context = null;
                }
            } catch (Throwable e) {
                logger.warn(e.getMessage(), e);
            }
        }
    }


    @Override
    public void close() throws IOException {
        logger.debug("close (no op)");
    }

    private void prepare() throws IOException, InterruptedException {
        logger.debug("prepare started");
        this.reloadSettings(settings); // reload settings to solve the schedule bug
        if (settings.getAsStructuredMap().containsKey("jdbc")) {
            settings = settings.getAsSettings("jdbc");
        }
        Runtime.getRuntime().addShutdownHook(shutdownHook());
        BlockingQueue<SettingsPipelineRequest> queue = new ArrayBlockingQueue<>(32);
        setQueue(queue);
        SettingsPipelineRequest element = new SettingsPipelineRequest().set(settings);
        getQueue().put(element);
        this.executorService = Executors.newFixedThreadPool(settings.getAsInt("concurrency", 1));
        logger.debug("prepare ended");
    }

    @Override
    public void newRequest(Pipeline<SettingsPipelineRequest> pipeline, SettingsPipelineRequest request) {
        try {
            process(request.get());
        } catch (Exception ex) {
            logger.error("error while processing request: " + ex.getMessage(), ex);
        }
    }

    private void process(Settings settings) throws Exception {
        if (context == null) {
            String strategy = settings.get("strategy", "standard");
            this.context = StrategyLoader.newContext(strategy);
            logger.info("strategy {}: settings = {}, context = {}",
                    strategy, settings.getAsMap(), context);
            context.setSettings(settings);
        }
        context.execute();
    }

    private List<Future> schedule(Settings settings) {
        List<Future> futures = new LinkedList<>();
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
            logger.info("scheduled with cron expressions {}", Arrays.asList(schedule));
        } else if (seconds > 0L) {
            Thread thread = new Thread(this);
            ScheduledThreadPoolExecutor scheduledThreadPoolExecutor =
                    new ScheduledThreadPoolExecutor(settings.getAsInt("threadpoolsize", 1));
            futures.add(scheduledThreadPoolExecutor.scheduleAtFixedRate(thread, 0L, seconds, TimeUnit.SECONDS));
            this.threadPoolExecutor = scheduledThreadPoolExecutor;
            logger.info("scheduled at fixed rate of {} seconds", seconds);
        }
        return futures;
    }

    private void execute() throws ExecutionException, InterruptedException {
        logger.debug("executing (queue={})", getQueue().size());
        new SimplePipelineExecutor<SettingsPipelineRequest, Pipeline<SettingsPipelineRequest>>(executorService)
                .setQueue(getQueue())
                .setPipelineProvider(pipelineProvider())
                .prepare()
                .execute()
                .waitFor();
        logger.debug("execution completed");
    }

    public synchronized void shutdown() throws Exception {
        if (shutdown) {
            return;
        }
        shutdown = true;
        // cancel scheduled runs
        if (futures != null) {
            for (Future future : futures) {
                future.cancel(true);
            }
        }
        // do no longer accept schedulings
        if (threadPoolExecutor != null) {
            threadPoolExecutor.shutdownNow();
            threadPoolExecutor = null;
        }
        executorService.shutdown();
        if (!executorService.awaitTermination(15, TimeUnit.SECONDS)) {
            executorService.shutdownNow();
            if (!executorService.awaitTermination(15, TimeUnit.SECONDS)) {
                throw new IOException("pool did not terminate");
            }
        }
        // shut down active context at last
        if (context != null) {
            context.shutdown();
        }
    }

    public boolean isShutdown() {
        return shutdown;
    }

    public void setContext(Context context) {
        this.context = context;
        setSettings(context.getSettings());
    }

    public Context getContext() {
        return context;
    }

    public Set<Context.State> getStates() {
        Set<Context.State> states = new HashSet<>();
        states.add(getContext().getState());
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
}
