package org.xbib.elasticsearch.common.metrics;

public class SinkMetric extends ElasticsearchIngestMetric {

    @Override
    public SinkMetric start() {
        super.start();
        return this;
    }
}
