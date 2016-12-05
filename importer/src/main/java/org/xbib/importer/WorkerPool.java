package org.xbib.importer;

import java.util.concurrent.BlockingQueue;

/**
 * Interface for a worker pool.
 *
 * @param <R> the request parameter type
 */
public interface WorkerPool<R>  {

    WorkerPool<R> open();

    BlockingQueue<R> getQueue();

    R getPoison();

    Worker<R> newWorker();

    void submit(R request);

}
