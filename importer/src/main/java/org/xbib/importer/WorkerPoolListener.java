package org.xbib.importer;

import java.util.Map;

/**
 * A callback listener for the state of the worker pool. Receives events of success or failure.
 *
 * @param <W> the worker pool type parameter
 */
public interface WorkerPoolListener<W extends WorkerPool<?>> {

    /**
     * Emits success if all workers were terminated without exception.
     * @param workerPool the worker pool
     */
    void success(W workerPool);

    /**
     * Emits a map of exceptions of all erraneous workers.
     * @param workerPool the worker pool
     * @param exceptions the worker exceptions
     */
    void failure(W workerPool, Map<Runnable, Throwable> exceptions);
}
