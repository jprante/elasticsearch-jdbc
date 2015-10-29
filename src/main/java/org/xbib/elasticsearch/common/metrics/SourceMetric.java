/*
 * Copyright (C) 2015 Jörg Prante
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
package org.xbib.elasticsearch.common.metrics;

import org.elasticsearch.common.metrics.CounterMetric;
import org.joda.time.DateTime;

import java.util.concurrent.atomic.AtomicInteger;

public class SourceMetric {

    private long started;

    private final CounterMetric totalRows = new CounterMetric();

    private final CounterMetric totalSizeInBytes = new CounterMetric();

    private final CounterMetric succeeded = new CounterMetric();

    private final CounterMetric failed = new CounterMetric();

    private final AtomicInteger counter = new AtomicInteger();

    private CounterMetric currentRows = new CounterMetric();

    public CounterMetric getTotalRows() {
        return totalRows;
    }

    public CounterMetric getTotalSizeInBytes() {
        return totalSizeInBytes;
    }

    public void resetCurrentRows() {
        currentRows = new CounterMetric();
    }

    public CounterMetric getCurrentRows() {
        return currentRows;
    }

    public CounterMetric getSucceeded() {
        return succeeded;
    }

    public CounterMetric getFailed() {
        return failed;
    }

    public SourceMetric start() {
        this.started = System.nanoTime();
        return this;
    }

    public long elapsed() {
        return System.nanoTime() - started;
    }

    private DateTime lastExecutionStart;

    public void setLastExecutionStart(DateTime dateTime) {
       this.lastExecutionStart = dateTime;
    }

    public DateTime getLastExecutionStart() {
        return lastExecutionStart;
    }

    private DateTime lastExecutionEnd;

    public void setLastExecutionEnd(DateTime dateTime) {
        this.lastExecutionEnd = dateTime;
    }

    public DateTime getLastExecutionEnd() {
        return lastExecutionEnd;
    }

    public void setCounter(int counter) {
        this.counter.getAndSet(counter);
    }

    public int getCounter() {
        return counter.get();
    }

    public void incCounter() {
        counter.incrementAndGet();
    }

}