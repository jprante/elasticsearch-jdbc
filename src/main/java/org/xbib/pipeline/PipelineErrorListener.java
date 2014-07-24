package org.xbib.pipeline;

public interface PipelineErrorListener<T, R extends PipelineRequest, E extends PipelineException> {

    /**
     * Receive an error from processing a pipeline request.
     *
     * @param pipeline the pipeline
     * @param request  the pipeline request
     * @param error    the pipeline error
     */
    void error(Pipeline<T, R> pipeline, R request, E error) throws PipelineException;
}
