
package org.xbib.pipeline;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;

/**
 * The Pipeline Executor can execute provided pipelines.
 * If the concurrency level is set to higher than one, more than one pipeline is executed in parallel.
 *
 * @param <R> the request type
 * @param <P> the pipeline type
 */
public interface PipelineExecutor<R extends PipelineRequest,P extends Pipeline<R>> {

    /**
     * Set the concurrency of this pipeline setExecutor
     * @param concurrency the concurrency, must be a positive integer
     * @return this setExecutor
     */
    PipelineExecutor<R,P> setConcurrency(int concurrency);

    /**
     * Set the provider of this pipeline setExecutor
     * @param provider the pipeline provider
     * @return this setExecutor
     */
    PipelineExecutor<R,P> setPipelineProvider(PipelineProvider<P> provider);

    PipelineExecutor<R,P> setQueue(BlockingQueue<R> queue);

    /**
     * Set pipeline sink
     * @param sink the pipeline sink
     * @return this setExecutor
     */
    PipelineExecutor<R,P> setSink(PipelineSink<R> sink);

    /**
     * Prepare the pipeline execution.
     * @return this setExecutor
     */
    PipelineExecutor<R,P> prepare();

    /**
     * Execute the pipelines.
     * @return this setExecutor
     */
    PipelineExecutor<R,P> execute();

    /**
     * Execute the pipelines.
     * @return this setExecutor
     * @throws InterruptedException
     * @throws ExecutionException
     * @throws IOException
     */
    PipelineExecutor<R,P> waitFor() throws InterruptedException, ExecutionException, IOException;

    /**
     * Shut down this pipeline executor.
     * @throws InterruptedException
     * @throws ExecutionException
     * @throws IOException
     */
    void shutdown() throws InterruptedException, ExecutionException, IOException;

    /**
     * Return pipelines
     * @return the pipelines
     */
    Collection<Pipeline<R>> getPipelines();
}
