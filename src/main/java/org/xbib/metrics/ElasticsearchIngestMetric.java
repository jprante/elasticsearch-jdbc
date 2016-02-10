package org.xbib.metrics;

import org.xbib.elasticsearch.helper.client.IngestMetric;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ElasticsearchIngestMetric implements IngestMetric {

    private final Set<String> indexNames = new HashSet<String>();
    private final Map<String, Long> startBulkRefreshIntervals = new HashMap<String, Long>();
    private final Map<String, Long> stopBulkRefreshIntervals = new HashMap<String, Long>();
    private final MeanMetric totalIngest = new ElasticsearchMeanMetric();
    private final CounterMetric totalIngestSizeInBytes = new ElasticsearchCounterMetric();
    private final CounterMetric currentIngest = new ElasticsearchCounterMetric();
    private final CounterMetric currentIngestNumDocs = new ElasticsearchCounterMetric();
    private final CounterMetric submitted = new ElasticsearchCounterMetric();
    private final CounterMetric succeeded = new ElasticsearchCounterMetric();
    private final CounterMetric failed = new ElasticsearchCounterMetric();
    private long started;

    public MeanMetric getTotalIngest() {
        return totalIngest;
    }

    public CounterMetric getTotalIngestSizeInBytes() {
        return totalIngestSizeInBytes;
    }

    public CounterMetric getCurrentIngest() {
        return currentIngest;
    }

    public CounterMetric getCurrentIngestNumDocs() {
        return currentIngestNumDocs;
    }

    public CounterMetric getSubmitted() {
        return submitted;
    }

    public CounterMetric getSucceeded() {
        return succeeded;
    }

    public CounterMetric getFailed() {
        return failed;
    }

    public ElasticsearchIngestMetric start() {
        this.started = System.nanoTime();
        return this;
    }

    public long elapsed() {
        return System.nanoTime() - started;
    }

    public ElasticsearchIngestMetric setupBulk(String indexName, long startRefreshInterval, long stopRefreshInterval) {
        synchronized (indexNames) {
            indexNames.add(indexName);
            startBulkRefreshIntervals.put(indexName, startRefreshInterval);
            stopBulkRefreshIntervals.put(indexName, stopRefreshInterval);
        }
        return this;
    }

    public boolean isBulk(String indexName) {
        return indexNames.contains(indexName);
    }

    public ElasticsearchIngestMetric removeBulk(String indexName) {
        synchronized (indexNames) {
            indexNames.remove(indexName);
        }
        return this;
    }

    public Set<String> indices() {
        return indexNames;
    }

    public Map<String, Long> getStartBulkRefreshIntervals() {
        return startBulkRefreshIntervals;
    }

    public Map<String, Long> getStopBulkRefreshIntervals() {
        return stopBulkRefreshIntervals;
    }

}
