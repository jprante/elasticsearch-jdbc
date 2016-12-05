package org.xbib.importer.util.concurrent;

import org.xbib.importer.util.CronExpression;

import java.util.Date;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Scheduled thread-pool executor implementation that leverages a CronExpression
 * to calculate future execution times for scheduled tasks.
 */
public class ExtendedCronThreadPoolExecutor extends ExtendedScheduledThreadPoolExecutor {

    private static final Logger logger = Logger.getLogger(ExtendedCronThreadPoolExecutor.class.getName());

    /**
     * Constructs a new ExtendedCronThreadPoolExecutor.
     *
     * @param corePoolSize the pool size
     * @param exceptionService exception service
     */
    public ExtendedCronThreadPoolExecutor(int corePoolSize, ExceptionService exceptionService) {
        super(corePoolSize, exceptionService);
    }

    public Future<?> schedule(final Runnable task, final CronExpression expression) {
        if (task == null) {
            throw new NullPointerException();
        }
        setCorePoolSize(getCorePoolSize() + 1);
        Runnable scheduleTask = () -> {
            Date now = new Date();
            Date time = expression.getNextValidTimeAfter(now);
            try {
                while (time != null) {
                    ExtendedCronThreadPoolExecutor.this.schedule(task, time.getTime() - now.getTime(), TimeUnit.MILLISECONDS);
                    while (now.before(time)) {
                        Thread.sleep(time.getTime() - now.getTime());
                        now = new Date();
                    }
                    time = expression.getNextValidTimeAfter(now);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.log(Level.SEVERE, e.getMessage(), e);
            } catch (RejectedExecutionException | CancellationException e) {
                logger.log(Level.SEVERE, e.getMessage(), e);
            }
        };
        return this.submit(scheduleTask);
    }

}
