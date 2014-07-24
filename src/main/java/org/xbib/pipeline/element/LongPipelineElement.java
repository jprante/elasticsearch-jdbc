package org.xbib.pipeline.element;

import org.xbib.pipeline.PipelineRequest;

import java.util.concurrent.atomic.AtomicLong;

public class LongPipelineElement implements PipelineElement<AtomicLong>, PipelineRequest {

    private AtomicLong n;

    @Override
    public AtomicLong get() {
        return n;
    }

    @Override
    public LongPipelineElement set(AtomicLong n) {
        this.n = n;
        return this;
    }

    @Override
    public String toString() {
        return n.toString();
    }
}
