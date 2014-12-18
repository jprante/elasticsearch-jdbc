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
package org.xbib.elasticsearch.plugin.jdbc.pipeline.executor;

import org.xbib.elasticsearch.plugin.jdbc.pipeline.Pipeline;
import org.xbib.elasticsearch.plugin.jdbc.pipeline.PipelineExecutor;
import org.xbib.elasticsearch.plugin.jdbc.pipeline.PipelineProvider;
import org.xbib.elasticsearch.plugin.jdbc.pipeline.PipelineRequest;

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
    public SimplePipelineExecutor<T, R, P> prepare() {
        if (provider == null) {
            throw new IllegalStateException("no provider set");
        }
        if (executorService == null) {
            this.executorService = Executors.newFixedThreadPool(concurrency);
        }
        if (concurrency < 1) {
            concurrency = 1;
        }
        this.pipelines = new LinkedList<P>();
        // limit to 256 to prevent unresponsive systems
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
        }
        return this;
    }

    @Override
    public void shutdown() {
        if (futures == null) {
            return;
        }
        for (Future<T> future : futures) {
            future.cancel(true);
        }
        if (executorService == null) {
            return;
        }
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(15, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            executorService.shutdownNow();
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
