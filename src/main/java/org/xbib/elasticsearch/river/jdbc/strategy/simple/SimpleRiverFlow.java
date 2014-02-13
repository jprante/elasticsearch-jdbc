
package org.xbib.elasticsearch.river.jdbc.strategy.simple;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;

import org.xbib.elasticsearch.river.jdbc.support.cron.CronExpression;
import org.xbib.elasticsearch.river.jdbc.support.cron.CronThreadPoolExecutor;
import org.xbib.elasticsearch.river.jdbc.RiverFlow;
import org.xbib.elasticsearch.river.jdbc.RiverMouth;
import org.xbib.elasticsearch.river.jdbc.RiverSource;
import org.xbib.elasticsearch.river.jdbc.RiverState;
import org.xbib.elasticsearch.river.jdbc.support.RiverContext;

import java.util.Date;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Simple river flow
 */
public class SimpleRiverFlow implements RiverFlow {

    private final ESLogger logger = ESLoggerFactory.getLogger(SimpleRiverFlow.class.getName());

    protected Thread thread;

    protected ScheduledThreadPoolExecutor executor;

    protected RiverContext context;

    protected RiverState state;

    protected boolean abort = false;

    protected ESLogger logger() {
        return logger;
    }

    @Override
    public String strategy() {
        return "simple";
    }

    @Override
    public SimpleRiverFlow riverContext(RiverContext context) {
        this.context = context;
        return this;
    }

    @Override
    public RiverContext riverContext() {
        return context;
    }

    @Override
    public void schedule(Thread thread) {
        this.thread = thread;
        if (context.getSchedule() != null) {
            // cron-like execution
            CronExpression cron = new CronExpression(context.getSchedule());
            CronThreadPoolExecutor executor = new CronThreadPoolExecutor(context.getPoolSize());
            this.executor = executor;
            executor.schedule(thread, cron);
        } else if (context.getInterval() != null && context.getInterval().millis() > 0) {
            // interval execution
            ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(context.getPoolSize());
            this.executor = executor;
            executor.scheduleAtFixedRate(thread, 0L, context.getInterval().millis(), TimeUnit.MILLISECONDS);
        } else {
            // single execution
            thread.start();
        }
    }

    @Override
    public void once(Thread thread) {
        this.thread = thread;
        thread.start();
    }

    /**
     * The river task run
     */
    @Override
    public void run() {
        move();
    }

    /**
     * A river move.
     */
    @Override
    public void move() {
        try {
            if (logger().isDebugEnabled()) {
                 logger().debug("begin of river move");
            }
            RiverSource source = context.riverSource();
            RiverMouth riverMouth = context.riverMouth();
            Client client = context.riverMouth().client();
            riverMouth.waitForCluster();
            if (state == null) {
                // create state object
                this.state = new RiverState()
                        .name("jdbc")
                        .setIndex("_river")
                        .setType(context.riverName())
                        .setId(ID_INFO_RIVER_INDEX)
                        .started(new Date().getTime())
                        .custom(context.asMap());
            }
            state.load(client);
            // increment state counter
            Long counter = state.counter() + 1;
            this.state = state.counter(counter)
                .active(true)
                .timestamp(new Date().getTime());
            state.save(client);
            if (logger().isDebugEnabled()) {
                logger().debug("state saved before fetch");
            }
            // set the job number to the state counter
            context.job(Long.toString(counter));
            if (logger().isDebugEnabled()) {
                logger().debug("trying to fetch ...");
            }
            source.fetch();
            if (logger().isDebugEnabled()) {
                logger().debug("... fetched, flushing");
            }
            riverMouth.flush();
            this.state = state.active(false)
                .timestamp(new Date().getTime());
            state.save(client);
        } catch (Exception e) {
            logger().error(e.getMessage(), e);
            // if any error occurs, abort the river
            abort();
        } finally {
            if (logger().isDebugEnabled()) {
                logger().debug("end of river move");
            }
        }
    }

    /**
     * Abort the river immediately.
     */
    @Override
    public void abort() {
        // do not abort twice
        if (abort) {
            return;
        }
        this.abort = true;
        logger().warn("aborting river");
        if (thread != null) {
            // kill thread
            thread.interrupt();
        }
        if (executor != null) {
            // do not wait for termination
            executor.shutdownNow();
        }
    }

    @Override
    public boolean isActive() {
        return thread != null && thread.isAlive() && !abort;
    }

    @Override
    public RiverState riverState() {
        return state;
    }
}
