package org.xbib.pipeline.simple;

import org.xbib.pipeline.Pipeline;
import org.xbib.pipeline.PipelineExecutor;
import org.xbib.pipeline.PipelineProvider;
import org.xbib.pipeline.PipelineRequest;
import org.xbib.pipeline.PipelineSink;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * A simple pipeline executor.
 *
 * @param <T> the pipeline result type
 * @param <R> the pipeline request type
 * @param <P> the pipeline type
 */
public class SimplePipelineExecutor<T, R extends PipelineRequest, P extends Pipeline<T, R>>
        implements PipelineExecutor<T, R, P> {

    private ExecutorService executorService;

    private Collection<P> pipelines;

    private Collection<Future<T>> futures;

    private PipelineProvider<P> provider;

    private PipelineSink<T> sink;

    private List<Throwable> exceptions;

    private int concurrency;

    @Override
    public SimplePipelineExecutor<T, R, P> setConcurrency(int concurrency) {
        this.concurrency = concurrency;
        return this;
    }

    @Override
    public SimplePipelineExecutor<T, R, P> setPipelineProvider(PipelineProvider<P> provider) {
        this.provider = provider;
        return this;
    }

    @Override
    public SimplePipelineExecutor<T, R, P> setSink(PipelineSink<T> sink) {
        this.sink = sink;
        return this;
    }

    @Override
    public SimplePipelineExecutor<T, R, P> prepare() {
        if (provider == null) {
            throw new IllegalStateException("no provider set");
        }
        if (executorService == null) {
            this.executorService = Executors.newFixedThreadPool(concurrency);
        }
        this.pipelines = new LinkedList<P>();
        if (concurrency < 1) {
            concurrency = 1;
        }
        for (int i = 0; i < Math.min(concurrency, 256); i++) {
            pipelines.add(provider.get());
        }
        return this;
    }

    /**
     * Execute pipelines
     *
     * @return this executor
     */
    @Override
    public SimplePipelineExecutor<T, R, P> execute() {
        if (pipelines == null) {
            prepare();
        }
        if (pipelines.isEmpty()) {
            throw new IllegalStateException("pipelines empty");
        }
        if (executorService == null) {
            this.executorService = Executors.newFixedThreadPool(concurrency);
        }
        futures = new LinkedList<Future<T>>();
        for (Callable<T> pipeline : pipelines) {
            futures.add(executorService.submit(pipeline));
        }
        return this;
    }

    /**
     * Wait for all results of the executions.
     *
     * @return this pipeline executor
     * @throws InterruptedException
     * @throws java.util.concurrent.ExecutionException
     */
    @Override
    public SimplePipelineExecutor<T, R, P> waitFor()
            throws InterruptedException, ExecutionException {
        if (executorService == null || pipelines == null || futures == null || futures.isEmpty()) {
            return this;
        }
        exceptions = new LinkedList<Throwable>();
        for (Future<T> future : futures) {
            T t = future.get();
            if (sink != null && !future.isCancelled()) {
                try {
                    sink.write(t);
                } catch (IOException e) {
                    exceptions.add(e);
                }
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

    /**
     * Get the pipelines of this executor.
     *
     * @return the pipelines
     */
    @Override
    public Collection<P> getPipelines() {
        return pipelines;
    }

    /**
     * Get the collected I/O exceptions that were thrown by the pipelines.
     *
     * @return list of exceptions
     */
    public List<Throwable> getExceptions() {
        return exceptions;
    }

}
