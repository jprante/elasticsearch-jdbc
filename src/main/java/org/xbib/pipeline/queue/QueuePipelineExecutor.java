package org.xbib.pipeline.queue;

import org.xbib.pipeline.Pipeline;
import org.xbib.pipeline.PipelineExecutor;
import org.xbib.pipeline.PipelineProvider;
import org.xbib.pipeline.PipelineRequest;
import org.xbib.pipeline.PipelineSink;
import org.xbib.pipeline.element.PipelineElement;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

/**
 * An Queue Pipeline setExecutor. This setExecutor can execute pipelines in parallel
 * and manage a queue that have to be processed by the pipelines.
 * <p/>
 * By doing this, the concurrency works on archive entry level, not URI level.
 * <p/>
 * Pipelines are created by a pipeline provider.
 * The maximum number of concurrent pipelines is 256.
 * Each pipeline can receive archive entries, which are put into a blocking queue by
 * this setExecutor.
 *
 * @param <T> the result type
 * @param <R> the pipeline request type
 * @param <P> the pipeline type
 * @param <E> the element type
 */
public class QueuePipelineExecutor<T, R extends PipelineRequest, P extends Pipeline<T, R>, E extends PipelineElement>
        implements PipelineExecutor<T, R, P> {

    private ExecutorService executorService;

    private PipelineProvider<P> provider;

    private Set<P> pipelines;

    private Collection<Future<T>> futures;

    private PipelineSink<T> sink;

    private CountDownLatch latch;

    private BlockingQueue<E> queue;

    private int concurrency;

    private volatile boolean closed;

    @Override
    public QueuePipelineExecutor<T, R, P, E> setConcurrency(int concurrency) {
        this.concurrency = concurrency;
        return this;
    }

    @Override
    public QueuePipelineExecutor<T, R, P, E> setPipelineProvider(PipelineProvider<P> provider) {
        this.provider = provider;
        return this;
    }

    @Override
    public QueuePipelineExecutor<T, R, P, E> setSink(PipelineSink<T> sink) {
        this.sink = sink;
        return this;
    }

    @Override
    public QueuePipelineExecutor<T, R, P, E> prepare() {
        if (provider == null) {
            throw new IllegalArgumentException("no provider set");
        }
        if (executorService == null) {
            this.executorService = Executors.newFixedThreadPool(concurrency);
        }
        if (pipelines == null) {
            this.pipelines = new HashSet<P>();
        }
        if (concurrency < 1) {
            concurrency = 1;
        }
        this.queue = new SynchronousQueue<E>(true); // true means fair queue
        this.latch = new CountDownLatch(concurrency);
        for (int i = 0; i < Math.min(concurrency, 256); i++) {
            P pipeline = provider.get();
            pipelines.add(pipeline);
        }
        return this;
    }

    /**
     * Usually, the pipelines receive elements from the setExecutor.
     * Here we ensure that all pipelines that receive elements are invoked.
     * This method returns immediately without waiting for the completion of pipelines.
     *
     * @return this setExecutor
     */
    @Override
    public QueuePipelineExecutor<T, R, P, E> execute() {
        futures = new LinkedList<Future<T>>();
        for (P pipeline : pipelines) {
            futures.add(executorService.submit(pipeline));
        }
        return this;
    }

    /**
     * Wait for all the pipelines executed.
     *
     * @return this setExecutor
     * @throws InterruptedException
     * @throws java.util.concurrent.ExecutionException
     */
    public QueuePipelineExecutor<T, R, P, E> waitFor() throws InterruptedException, ExecutionException, IOException {
        for (Future<T> future : futures) {
            T t = future.get();
            if (sink != null) {
                sink.write(t);
            }
        }
        return this;
    }

    @Override
    public void shutdown() throws InterruptedException, IOException {
        if (executorService == null) {
            return;
        }
        executorService.shutdown();
        if (!executorService.awaitTermination(15, TimeUnit.SECONDS)) {
            executorService.shutdownNow();
            if (!executorService.awaitTermination(15, TimeUnit.SECONDS)) {
                throw new IOException("pool did not terminate");
            }
        }
    }

    public void shutdown(E poisonElement) throws InterruptedException, ExecutionException, IOException {
        if (closed) {
            return;
        }
        closed = true;
        // how many pipelines are still running?
        int active = 0;
        for (P pipeline : pipelines) {
            if (pipeline.isRunning()) {
                active++;
            }
        }
        for (int i = 0; i < active; i++) {
            queue.offer(poisonElement, 30, TimeUnit.SECONDS);
        }
        waitFor();
        shutdown();
    }

    /**
     * Get queue
     *
     * @return the queue
     */
    public BlockingQueue<E> queue() {
        return queue;
    }

    /**
     * Get pipelines
     *
     * @return the pipelines
     */
    public Set<P> getPipelines() {
        return pipelines;
    }

    /**
     * Count down the latch. Decreases the number of active pipelines.
     * Called from a pipeline when it terminates.
     *
     * @return this setExecutor
     */
    public QueuePipelineExecutor countDown() {
        latch.countDown();
        return this;
    }

    /**
     * Returns the number of pipelines.  If this pipeline setExecutor can receive requests,
     * the returned number is greater than 0.
     *
     * @return number of pipelines ready to receive requests
     */
    public long canReceive() {
        return latch.getCount();
    }

}
