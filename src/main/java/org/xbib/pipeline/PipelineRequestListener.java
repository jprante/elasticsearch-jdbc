package org.xbib.pipeline;

public interface PipelineRequestListener<T, R extends PipelineRequest> {

    /**
     * Receive a new request in this pipeline.
     *
     * @param pipeline the pipeline
     * @param request  the pipeline request
     */
    void newRequest(Pipeline<T, R> pipeline, R request) throws PipelineException;
}
