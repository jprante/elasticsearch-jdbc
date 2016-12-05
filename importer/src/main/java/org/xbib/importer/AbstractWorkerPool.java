package org.xbib.importer;

import org.xbib.importer.util.concurrent.ExceptionService;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A worker pool for processing request by a number of worker threads.
 * If worker threads exit early, they are removed and finished, not reused.
 * If no worker is left, the pool closes.
 *
 * @param <R> the request type
 */
public abstract class AbstractWorkerPool<R> implements WorkerPool<R>, AutoCloseable {

    private static final int DEFAULT_WAIT_SECONDS = 30;

    private final ThreadPoolExecutor executor;

    private final ExceptionService exceptionService;

    private final BlockingQueue<R> queue;

    private final int workerCount;

    private final int waitSeconds;

    private final AtomicBoolean closed;

    private final CountDownLatch latch;

    private final WorkerPoolListener<WorkerPool<R>> listener;

    public AbstractWorkerPool(int workerCount, ThreadPoolExecutor executor, ExceptionService exceptionService) {
        this(workerCount, executor, exceptionService, null);
    }

    public AbstractWorkerPool(int workerCount, ThreadPoolExecutor executor, ExceptionService exceptionService,
                              WorkerPoolListener<WorkerPool<R>> listener) {
        this(workerCount, executor, exceptionService, listener, DEFAULT_WAIT_SECONDS);
    }

    public AbstractWorkerPool(int workerCount, ThreadPoolExecutor executor, ExceptionService exceptionService,
                              WorkerPoolListener<WorkerPool<R>> listener, int waitSeconds) {
        this.workerCount = workerCount;
        this.waitSeconds = waitSeconds;
        this.listener = listener;
        this.executor = executor;
        this.exceptionService = exceptionService;
        this.queue = new SynchronousQueue<>(true);
        this.closed = new AtomicBoolean(true);
        this.latch = new CountDownLatch(workerCount);
    }

    public ThreadPoolExecutor getExecutor() {
        return executor;
    }

    public ExceptionService getExceptionService() {
        return exceptionService;
    }

    public Runnable wrap(Worker<R> worker) {
        return new Wrapper(worker);
    }

    @Override
    public WorkerPool<R> open() {
        if (closed.compareAndSet(true, false)) {
            for (int i = 0; i < workerCount; i++) {
                getExecutor().submit(wrap(newWorker()));
            }
        }
        return this;
    }

    @Override
    public BlockingQueue<R> getQueue() {
        return queue;
    }

    @Override
    public void submit(R request) {
        if (closed.get()) {
            throw new UncheckedIOException(new IOException("closed"));
        }
        try {
            if (latch.getCount() == 0) {
                throw new UncheckedIOException(new IOException("no worker available"));
            }
            if (request.equals(getPoison())) {
                throw new UncheckedIOException(new IOException("ignoring poison"));
            }
            queue.put(request);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new UncheckedIOException(new IOException(e));
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            while (latch.getCount() > 0) {
                try {
                    queue.put(getPoison());
                    // wait for latch being updated by other thread
                    Thread.sleep(50L);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            try {
                getExecutor().shutdownNow();
                getExecutor().awaitTermination(waitSeconds, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new UncheckedIOException(new IOException(e));
            } finally {
                if (listener != null) {
                    if (getExceptionService().getExceptions().isEmpty()) {
                        listener.success(this);
                    } else {
                        listener.failure(this, getExceptionService().getExceptions());
                    }
                }
            }
        }
    }

    private class Wrapper implements Runnable {

        private final Logger logger = Logger.getLogger(Worker.class.getName());

        private final Worker<R> worker;

        private int counter;

        private Wrapper(Worker<R> worker) {
            this.worker = worker;
        }

        @Override
        public void run() {
            R request = null;
            try {
                logger.log(Level.INFO, "start of worker " + worker);
                while (true) {
                    request = getQueue().take();
                    if (getPoison().equals(request)) {
                        break;
                    }
                    worker.execute(request);
                    counter++;
                }
            } catch (InterruptedException e) {
                // we got interrupted, this may lead to data loss. Clear interrupt state and log warning.
                Thread.currentThread().interrupt();
                logger.log(Level.WARNING, e.getMessage(), e);
                getExceptionService().getExceptions().put(this, e);
            } catch (Exception e) {
                // catch unexpected exception. Throwables, Errors are examined in afterExecute.
                logger.log(Level.SEVERE, e.getMessage(), e);
                getExceptionService().getExceptions().put(this, e);
                throw new UncheckedIOException(new IOException(e));
            } finally {
                latch.countDown();
                logger.log(Level.INFO, "end of worker " +
                        worker + " " + (getPoison().equals(request) ? "(completed, " + counter + " requests)" :
                                "(abnormal termination after " + counter + " requests)"));
            }
        }
    }
}
