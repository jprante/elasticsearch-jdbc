package org.xbib.pipeline;

import java.io.IOException;

public interface PipelineSink<T> {

    void write(T t) throws IOException;
}
