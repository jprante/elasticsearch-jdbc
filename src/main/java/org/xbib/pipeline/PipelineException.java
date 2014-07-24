package org.xbib.pipeline;

public class PipelineException extends Exception {

    public PipelineException(String msg) {
        super(msg);
    }

    public PipelineException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
