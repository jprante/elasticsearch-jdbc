
package org.xbib.pipeline;

import org.elasticsearch.common.metrics.MeterMetric;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class MetricSimplePipelineExecutor<R extends PipelineRequest, P extends Pipeline<R>>
        extends SimplePipelineExecutor<R,P> {

    private final static ScheduledExecutorService service = Executors.newScheduledThreadPool(3);

    protected MeterMetric metric;

    @Override
    public MetricSimplePipelineExecutor<R,P> setConcurrency(int concurrency) {
        super.setConcurrency(concurrency);
        return this;
    }

    @Override
    public MetricSimplePipelineExecutor<R,P> setPipelineProvider(PipelineProvider<P> provider) {
        super.setPipelineProvider(provider);
        return this;
    }

    @Override
    public MetricSimplePipelineExecutor<R,P> setQueue(BlockingQueue<R> queue) {
        super.setQueue(queue);
        return this;
    }

    @Override
    public MetricSimplePipelineExecutor<R,P> setSink(PipelineSink<R> sink) {
        super.setSink(sink);
        return this;
    }

    @Override
    public MetricSimplePipelineExecutor<R,P> prepare() {
        super.prepare();
        return this;
    }

    @Override
    public MetricSimplePipelineExecutor<R,P> execute() {
        metric = newMetric();
        super.execute();
        return this;
    }

    @Override
    public MetricSimplePipelineExecutor<R,P> waitFor()
            throws InterruptedException, ExecutionException {
        super.waitFor();
        metric.stop();
        return this;
    }

    public MeterMetric newMetric() {
        return new MeterMetric(service, TimeUnit.SECONDS);
    }

    public MeterMetric metric() {
        return metric;
    }
}
