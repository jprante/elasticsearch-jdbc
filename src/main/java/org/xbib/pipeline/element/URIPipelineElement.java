package org.xbib.pipeline.element;

import org.xbib.pipeline.PipelineRequest;

import java.net.URI;

public class URIPipelineElement implements PipelineElement<URI>, PipelineRequest {

    private URI uri;

    @Override
    public URI get() {
        return uri;
    }

    @Override
    public URIPipelineElement set(URI uri) {
        this.uri = uri;
        return this;
    }

    @Override
    public String toString() {
        return uri.toString();
    }
}