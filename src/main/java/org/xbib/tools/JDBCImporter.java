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
import org.xbib.elasticsearch.jdbc.strategy.Context;
import org.xbib.elasticsearch.common.util.StrategyLoader;
import org.xbib.pipeline.Pipeline;
import org.xbib.pipeline.PipelineProvider;

import java.io.IOException;
import java.security.Security;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;

import static org.elasticsearch.common.collect.Queues.newConcurrentLinkedQueue;

/**
 * Standalone importer for JDBC
 */
public class JDBCImporter extends Importer {

    private final static Logger logger = LogManager.getLogger("importer.jdbc");

    private final static JDBCImporter INSTANCE = new JDBCImporter();

    protected Context context;

    private volatile boolean closed;

    public static JDBCImporter getInstance() {
        return INSTANCE;
    }

    @Override
    protected PipelineProvider<Pipeline> pipelineProvider() {
        return new PipelineProvider<Pipeline>() {
            @Override
            public Pipeline get() {
                return INSTANCE;
            }
        };
    }

    @Override
    protected void prepare() throws IOException {
        logger.debug("prepare started");
        if (settings.getAsStructuredMap().containsKey("jdbc")) {
            settings = settings.getAsSettings("jdbc");
        }
        Security.setProperty("networkaddress.cache.ttl", "0");
        Runtime.getRuntime().addShutdownHook(shutdownHook());
        ingest = createIngest();
        String index = settings.get("index");
        if (index != null) {
            setIndex(index);
            int pos = index.indexOf('\'');
            if (pos >= 0) {
                SimpleDateFormat formatter = new SimpleDateFormat();
                formatter.applyPattern(index);
                index = formatter.format(new Date());
            }
            index = resolveAlias(index);
            setConcreteIndex(index);
            logger.info("index name = {}, concrete index name = {}", getIndex(), getConcreteIndex());
        }
        Integer maxbulkactions = settings.getAsInt("max_bulk_actions", 1000);
        Integer maxconcurrentbulkrequests = settings.getAsInt("max_concurrent_bulk_requests",
                Runtime.getRuntime().availableProcessors());
        ingest.maxActionsPerBulkRequest(maxbulkactions)
                .maxConcurrentBulkRequests(maxconcurrentbulkrequests);
        createIndex(getConcreteIndex());
        input = newConcurrentLinkedQueue();
        input.offer(settings);
        logger.debug("prepare ended");
    }

    @Override
    protected List<Future> schedule(Settings settings) {
        if (settings.getAsStructuredMap().containsKey("jdbc")) {
            settings = settings.getAsSettings("jdbc");
        }
        return super.schedule(settings);
    }

    @Override
    protected void process(Settings settings) throws Exception {
        if (settings.getAsStructuredMap().containsKey("jdbc")) {
            settings = settings.getAsSettings("jdbc");
        }
        if (context == null) {
            String strategy = settings.get("strategy", "standard");
            this.context = StrategyLoader.newContext(strategy);
            logger.info("strategy {}: settings = {}, context = {}",
                    strategy, settings.getAsMap(), context);
            context.setSettings(settings);
        }
       
        context.execute();
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
        if (INSTANCE.getContext() != null) {
            states.add(INSTANCE.getContext().getState());
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
        super.shutdown();
        if (context != null) {
            context.shutdown();
        }
        if (ingest != null) {
            ingest.shutdown();
            ingest = null;
        }
    }

    public boolean isShutdown() {
        return closed;
    }
}
