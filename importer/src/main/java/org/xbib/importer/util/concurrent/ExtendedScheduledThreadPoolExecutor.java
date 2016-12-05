package org.xbib.importer.util.concurrent;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 */
public class ExtendedScheduledThreadPoolExecutor extends ScheduledThreadPoolExecutor {

    private static final Logger logger = Logger.getLogger(ExtendedThreadPoolExecutor.class.getName());

    private final ExceptionService exceptionService;

    public ExtendedScheduledThreadPoolExecutor(int corePoolSize, ExceptionService exceptionService) {
        super(corePoolSize);
        this.exceptionService = exceptionService;
    }

    /*
     * Examine Throwable or Error of a thread after execution just to log them.
     */
    @Override
    protected void afterExecute(Runnable runnable, Throwable terminationCause) {
        super.afterExecute(runnable, terminationCause);
        Throwable throwable = terminationCause;
        if (throwable == null && runnable instanceof Future<?>) {
            try {
                Future<?> future = (Future<?>) runnable;
                if (future.isDone()) {
                    future.get();
                }
            } catch (CancellationException ce) {
                logger.log(Level.SEVERE, ce.getMessage(), ce);
                throwable = ce;
            } catch (ExecutionException ee) {
                logger.log(Level.SEVERE, ee.getMessage(), ee);
                throwable = ee.getCause();
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                logger.log(Level.SEVERE, ie.getMessage(), ie);
            }
        }
        if (throwable != null) {
            logger.log(Level.SEVERE, throwable.getMessage(), throwable);
            exceptionService.setException(runnable, throwable);
        }
    }
}
