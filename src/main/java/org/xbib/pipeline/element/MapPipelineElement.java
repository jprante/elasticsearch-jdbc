package org.xbib.pipeline.element;

import org.xbib.pipeline.PipelineRequest;

import java.util.Map;

public class MapPipelineElement implements PipelineElement<Map<String, Object>>, PipelineRequest {

    private Map<String, Object> map;

    @Override
    public Map<String, Object> get() {
        return map;
    }

    @Override
    public MapPipelineElement set(Map<String, Object> map) {
        this.map = map;
        return this;
    }

    @Override
    public String toString() {
        return map.toString();
    }
}