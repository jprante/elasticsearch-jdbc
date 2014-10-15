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

import org.elasticsearch.common.metrics.MeterMetric;
import org.xbib.elasticsearch.plugin.jdbc.pipeline.Pipeline;
import org.xbib.elasticsearch.plugin.jdbc.pipeline.PipelineProvider;
import org.xbib.elasticsearch.plugin.jdbc.pipeline.PipelineRequest;

import java.util.concurrent.ExecutionException;

public class MetricSimplePipelineExecutor<T, R extends PipelineRequest, P extends Pipeline<T, R>>
        extends SimplePipelineExecutor<T, R, P> {

    private final MeterMetric metric;

    public MetricSimplePipelineExecutor(MeterMetric meterMetric) {
        this.metric = meterMetric;
    }

    @Override
    public MetricSimplePipelineExecutor<T, R, P> setConcurrency(int concurrency) {
        super.setConcurrency(concurrency);
        return this;
    }

    @Override
    public MetricSimplePipelineExecutor<T, R, P> setPipelineProvider(PipelineProvider<P> provider) {
        super.setPipelineProvider(provider);
        return this;
    }

    @Override
    public MetricSimplePipelineExecutor<T, R, P> prepare() {
        super.prepare();
        return this;
    }

    @Override
    public MetricSimplePipelineExecutor<T, R, P> execute() {
        super.execute();
        return this;
    }

    @Override
    public MetricSimplePipelineExecutor<T, R, P> waitFor()
            throws InterruptedException, ExecutionException {
        super.waitFor();
        metric.stop();
        return this;
    }

    public MeterMetric metric() {
        return metric;
    }
}
