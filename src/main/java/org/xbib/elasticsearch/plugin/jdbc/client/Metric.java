/*
 * Copyright (C) 2014 Jörg Prante
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.xbib.elasticsearch.plugin.jdbc.client;

import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.metrics.MeanMetric;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Metric {

    private long started;

    private final Set<String> indexNames = new HashSet<String>();

    private final Map<String, Long> startBulkRefreshIntervals = new HashMap<String, Long>();

    private final Map<String, Long> stopBulkRefreshIntervals = new HashMap<String, Long>();

    private final MeanMetric totalIngest = new MeanMetric();

    private final CounterMetric totalIngestSizeInBytes = new CounterMetric();

    private final CounterMetric currentIngest = new CounterMetric();

    private final CounterMetric currentIngestNumDocs = new CounterMetric();

    private final CounterMetric submitted = new CounterMetric();

    private final CounterMetric succeeded = new CounterMetric();

    private final CounterMetric failed = new CounterMetric();

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

    public Metric start() {
        this.started = System.nanoTime();
        return this;
    }

    public long elapsed() {
        return System.nanoTime() - started;
    }

    public Metric setupBulk(String indexName, long startRefreshInterval, long stopRefreshInterval) {
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

    public Metric removeBulk(String indexName) {
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
