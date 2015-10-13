
package org.xbib.pipeline;

import java.io.IOException;

public interface PipelineSink<R extends PipelineRequest> {

    void sink(R request) throws IOException;
}
