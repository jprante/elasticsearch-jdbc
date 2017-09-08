
package org.xbib.pipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Base class for {@link Pipeline} implementations.
 *
 * @param <R> the pipeline request type
 */
public abstract class AbstractPipeline<R extends PipelineRequest> implements Pipeline<R> {

    private final static Logger logger = LoggerFactory.getLogger(AbstractPipeline.class);

    private BlockingQueue<R> queue;  //= new SynchronousQueue<>(true);

    @Override
    public Pipeline<R> setQueue(BlockingQueue<R> queue) {
        if (queue == null) {
            throw new IllegalArgumentException();
        }
        this.queue = queue;
        return this;
    }

    public BlockingQueue<R> getQueue() {
        return queue;
    }

    /**
     * Call this thread. Take next request and pass them to request listeners.
     * At least, this pipeline itself can listen to requests and handle errors.
     * Only PipelineExceptions are handled for each listener. Other execptions will quit the
     * pipeline request executions.
     * @return a metric about the pipeline request executions.
     * @throws Exception if pipeline execution was sborted by a non-PipelineException
     */
    @Override
    public R call() throws Exception {
        R r = null;
        try {
            r = queue.poll(5L, TimeUnit.SECONDS);
            while (r != null) {
                newRequest(this, r);
                r = queue.poll(5L, TimeUnit.SECONDS);
            }
            close();
        } catch (InterruptedException e) {
            logger.warn("interrupted");
        } catch (Throwable t) {
            logger.error(t.getMessage(), t);
            throw t;
        }
        return r;
    }

    /**
     * A new request for the pipeline is processed.
     * @param pipeline the pipeline
     * @param request the pipeline request
     */
    public abstract void newRequest(Pipeline<R> pipeline, R request);

}
