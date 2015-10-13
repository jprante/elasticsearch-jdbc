
package org.xbib.pipeline;

public interface PipelineProvider<P extends Pipeline> {

    /**
     * Get a new instance of a pipeline
     * @return a new pipeline instance
     */
    P get();

}
