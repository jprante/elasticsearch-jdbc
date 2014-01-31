
package org.xbib.elasticsearch.river.jdbc.support.cron;

import java.util.concurrent.ExecutorService;

/**
 * Executor service that schedules a task for execution via a cron expression.
 */
public interface CronExecutorService extends ExecutorService {
    /**
     * Schedules the specified task to execute according to the specified cron expression.
     *
     * @param task       the Runnable task to schedule
     * @param expression a cron expression
     */
    void schedule(Runnable task, CronExpression expression);
}
