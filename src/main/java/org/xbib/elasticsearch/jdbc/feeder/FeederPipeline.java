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
package org.xbib.elasticsearch.jdbc.feeder;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.xbib.elasticsearch.common.pipeline.Pipeline;
import org.xbib.elasticsearch.common.pipeline.element.ContextPipelineElement;
import org.xbib.elasticsearch.jdbc.strategy.Context;
import org.xbib.elasticsearch.jdbc.strategy.Flow;

import java.util.Queue;

/**
 * A feeder pipeline is a collection of threads that can be executed in parallel
 */
public class FeederPipeline implements Pipeline<Boolean, ContextPipelineElement> {

    private final static ESLogger logger = ESLoggerFactory.getLogger("jdbc");

    private final Flow flow;

    private Context context;

    private volatile boolean interrupted;

    public FeederPipeline(Flow flow) {
        this.flow = flow;
    }

    public void setInterrupted(boolean interrupted) {
        this.interrupted = interrupted;
    }

    public boolean isInterrupted() {
        return interrupted;
    }

    /**
     * Call this thread. Iterate over all request and pass them to request listeners.
     * At least, this pipeline itself can listen to requests and handle errors.
     * Only PipelineExceptions are handled for each listener. Other execptions will quit the
     * pipeline request executions.
     *
     * @return a metric about the pipeline request executions.
     * @throws Exception if pipeline execution was sborted by a non-PipelineException
     */
    @Override
    public Boolean call() throws Exception {
        interrupted = false;
        while (hasNext() && !interrupted) {
            ContextPipelineElement r = next();
            request(this, r);
        }
        flow.logMetrics(context, "pipeline " + this + " complete");
        logger.debug("releasing context");
        context.release();
        return true;
    }

    /**
     * Removing pipeline requests is not supported.
     */
    @Override
    public void remove() {
        throw new UnsupportedOperationException("remove not supported");
    }

    private void request(Pipeline<Boolean, ContextPipelineElement> pipeline, ContextPipelineElement request) {
        try {
            if (context != null) {
                flow.logMetrics(context, "next request for pipeline " + this);
            }
            this.context = request.get();
            flow.execute(context);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    @Override
    public boolean hasNext() {
        return !flow.getQueue().isEmpty();
    }

    @Override
    public ContextPipelineElement next() {
        Queue<Context> queue = flow.getQueue();
        return new ContextPipelineElement().set(queue.poll());
    }

    public Context getContext() {
        return context;
    }

}
