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
package org.xbib.elasticsearch.plugin.jdbc.pipeline;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ExecutionException;

/**
 * The Pipeline Executor can execute provided pipelines.
 * If the concurrency level is set to higher than one, more than one pipeline is executed in parallel.
 *
 * @param <T> the result type
 * @param <R> the request type
 * @param <P> the pipeline type
 */
public interface PipelineExecutor<T, R extends PipelineRequest, P extends Pipeline<T, R>> {

    /**
     * Set the concurrency of this pipeline setExecutor
     *
     * @param concurrency the concurrency, must be a positive integer
     * @return this setExecutor
     */
    PipelineExecutor<T, R, P> setConcurrency(int concurrency);

    /**
     * Set the provider of this pipeline setExecutor
     *
     * @param provider the pipeline provider
     * @return this setExecutor
     */
    PipelineExecutor<T, R, P> setPipelineProvider(PipelineProvider<P> provider);

    /**
     * Prepare the pipeline execution.
     *
     * @return this setExecutor
     */
    PipelineExecutor<T, R, P> prepare();

    /**
     * Execute the pipelines.
     *
     * @return this setExecutor
     */
    PipelineExecutor<T, R, P> execute();

    /**
     * Execute the pipelines.
     *
     * @return this setExecutor
     * @throws InterruptedException
     * @throws java.util.concurrent.ExecutionException
     * @throws java.io.IOException
     */
    PipelineExecutor<T, R, P> waitFor() throws InterruptedException, ExecutionException, IOException;

    /**
     * Shut down this pipeline executor.
     *
     * @throws InterruptedException
     * @throws java.util.concurrent.ExecutionException
     * @throws java.io.IOException
     */
    void shutdown() throws InterruptedException, ExecutionException, IOException;

    /**
     * Return pipelines
     *
     * @return the pipelines
     */
    Collection<P> getPipelines();
}
