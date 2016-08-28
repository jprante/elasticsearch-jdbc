package org.xbib.pipeline;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * A simple pipeline executor.
 * @param <R> the pipeline request type
 * @param <P> the pipeline type
 */
public class SimplePipelineExecutor<R extends PipelineRequest, P extends Pipeline<R>>
    implements PipelineExecutor<R,P> {

    private final ExecutorService executorService;

    private BlockingQueue<R> queue;

    private Collection<Pipeline<R>> pipelines;

    private Collection<Future<R>> futures;

    private PipelineProvider<P> provider;

    private PipelineSink<R> sink;

    private List<Throwable> exceptions;

    private int concurrency;

    public SimplePipelineExecutor(ExecutorService executorService) {
        this.executorService = executorService;
    }

    @Override
    public SimplePipelineExecutor<R,P> setConcurrency(int concurrency) {
        this.concurrency = concurrency;
        return this;
    }

    @Override
    public SimplePipelineExecutor<R,P> setPipelineProvider(PipelineProvider<P> provider) {
        this.provider = provider;
        return this;
    }

    @Override
    public SimplePipelineExecutor<R,P> setQueue(BlockingQueue<R> queue) {
        if (queue == null) {
            throw new IllegalArgumentException("null queue is not accepted");
        }
        this.queue = queue;
        return this;
    }

    @Override
    public SimplePipelineExecutor<R,P> setSink(PipelineSink<R> sink) {
        this.sink = sink;
        return this;
    }

    @Override
    public SimplePipelineExecutor<R,P> prepare() {
        if (provider == null) {
            throw new IllegalStateException("no provider set");
        }
        if (queue == null) {
            throw new IllegalStateException("no queue set");
        }
        this.pipelines = new LinkedList<>();
        if (concurrency < 1) {
            concurrency = 1;
        }
        for (int i = 0; i < Math.min(concurrency, 256); i++) {
            pipelines.add(provider.get().setQueue(queue));
        }
        return this;
    }

    /**
     * Execute pipelines
     * @return this executor
     */
    @Override
    public SimplePipelineExecutor<R,P> execute() {
        if (pipelines == null) {
            prepare();
        }
        if (pipelines.isEmpty()) {
            throw new IllegalStateException("pipelines empty");
        }
        futures = new LinkedList<>();
        for (Callable<R> pipeline : pipelines) {
            futures.add(executorService.submit(pipeline));
        }
        return this;
    }

    /**
     * Wait for all results of the executions.
     *
     * @return this pipeline executor
     * @throws InterruptedException
     * @throws ExecutionException
     */
    @Override
    public SimplePipelineExecutor<R,P> waitFor()
            throws InterruptedException, ExecutionException {
        if (executorService == null || pipelines == null || futures == null || futures.isEmpty()) {
            return this;
        }
        exceptions = new LinkedList<>();
        for (Future<R> future : futures) {
            R r = future.get();
            if (sink != null && !future.isCancelled()) {
                try {
                    sink.sink(r);
                } catch (IOException e) {
                    exceptions.add(e);
                }
            }
        }
        return this;
    }

    @Override
    public void shutdown() throws InterruptedException, IOException {
    }

    /**
     * Get the pipelines of this executor.
     * @return the pipelines
     */
    @Override
    public Collection<Pipeline<R>> getPipelines() {
        return pipelines;
    }

    /**
     * Get the collected I/O exceptions that were thrown by the pipelines.
     * @return list of exceptions
     */
    public List<Throwable> getExceptions() {
        return exceptions;
    }

}
