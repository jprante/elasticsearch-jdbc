
package org.xbib.elasticsearch.river.jdbc.support.cron;

import java.util.Date;
import java.util.concurrent.CancellationException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Scheduled thread-pool executor implementation that leverages a CronExpression
 * to calculate future execution times for scheduled tasks.
 */
public class CronThreadPoolExecutor extends ScheduledThreadPoolExecutor implements CronExecutorService {
    /**
     * Constructs a new CronThreadPoolExecutor.
     *
     * @param corePoolSize the pool size
     */
    public CronThreadPoolExecutor(int corePoolSize) {
        super(corePoolSize);
    }

    /**
     * Constructs a new CronThreadPoolExecutor.
     *
     * @param corePoolSize the pool size
     */
    public CronThreadPoolExecutor(int corePoolSize, ThreadFactory threadFactory) {
        super(corePoolSize, threadFactory);
    }

    /**
     * Constructs a new CronThreadPoolExecutor.
     *
     * @param corePoolSize the pool size
     */
    public CronThreadPoolExecutor(int corePoolSize, RejectedExecutionHandler handler) {
        super(corePoolSize, handler);
    }

    /**
     * Constructs a new CronThreadPoolExecutor.
     *
     * @param corePoolSize the pool size
     * @param handler the handler for rejecting executions
     */
    public CronThreadPoolExecutor(int corePoolSize, ThreadFactory threadFactory, RejectedExecutionHandler handler) {
        super(corePoolSize, threadFactory, handler);
    }

    @Override
    public void schedule(final Runnable task, final CronExpression expression) {
        if (task == null) {
            throw new NullPointerException();
        }
        this.setCorePoolSize(this.getCorePoolSize() + 1);
        Runnable scheduleTask = new Runnable() {
            /**
             * @see Runnable#run()
             */
            @Override
            public void run() {
                Date now = new Date();
                Date time = expression.getNextValidTimeAfter(now);
                try {
                    while (time != null) {
                        CronThreadPoolExecutor.this.schedule(task, time.getTime() - now.getTime(), TimeUnit.MILLISECONDS);
                        while (now.before(time)) {
                            Thread.sleep(time.getTime() - now.getTime());
                            now = new Date();
                        }
                        time = expression.getNextValidTimeAfter(now);
                    }
                } catch (RejectedExecutionException e) {
                    //
                } catch (CancellationException e) {
                    //
                } catch (InterruptedException e) {
                    //
                    Thread.currentThread().interrupt();
                }
            }
        };
        this.execute(scheduleTask);
    }
}
