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
package org.xbib.elasticsearch.common.cron;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;

import java.util.Date;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Future;
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

    private final static ESLogger logger = ESLoggerFactory.getLogger("jdbc");

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
     * @param corePoolSize  the pool size
     * @param threadFactory the thread factory
     */
    public CronThreadPoolExecutor(int corePoolSize, ThreadFactory threadFactory) {
        super(corePoolSize, threadFactory);
    }

    /**
     * Constructs a new CronThreadPoolExecutor.
     *
     * @param corePoolSize the pool size
     * @param handler      the handler for rejected executions
     */
    public CronThreadPoolExecutor(int corePoolSize, RejectedExecutionHandler handler) {
        super(corePoolSize, handler);
    }

    /**
     * Constructs a new CronThreadPoolExecutor.
     *
     * @param corePoolSize  the pool size
     * @param handler       the handler for rejecting executions
     * @param threadFactory the thread factory
     */
    public CronThreadPoolExecutor(int corePoolSize, ThreadFactory threadFactory, RejectedExecutionHandler handler) {
        super(corePoolSize, threadFactory, handler);
    }

    @Override
    public Future<?> schedule(final Runnable task, final CronExpression expression) {
        if (task == null) {
            throw new NullPointerException();
        }
        setCorePoolSize(getCorePoolSize() + 1);
        Runnable scheduleTask = new Runnable() {
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
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (RejectedExecutionException | CancellationException e) {
                    logger.error(e.getMessage(), e);
                }
            }
        };
        return this.submit(scheduleTask);
    }
}
