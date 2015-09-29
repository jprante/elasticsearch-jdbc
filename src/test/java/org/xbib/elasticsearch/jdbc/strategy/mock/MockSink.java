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
package org.xbib.elasticsearch.jdbc.strategy.mock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Settings;
import org.xbib.elasticsearch.common.util.IndexableObject;
import org.xbib.elasticsearch.jdbc.strategy.Sink;
import org.xbib.elasticsearch.support.client.Metric;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class MockSink implements Sink<MockContext> {

    private final static Logger logger = LogManager.getLogger(MockSink.class);

    private Map<IndexableObject, String> data;

    private long counter;

    @Override
    public String strategy() {
        return "mock";
    }

    @Override
    public Sink<MockContext> newInstance() {
        return new MockSink();
    }

    @Override
    public void beforeFetch() throws Exception {
    }

    @Override
    public void afterFetch() throws Exception {

    }

    public MockSink() {
        data = new TreeMap<IndexableObject, String>();
        counter = 0L;
    }

    @Override
    public void index(IndexableObject object, boolean create) throws IOException {
        logger.info("index {} = {}", object.toString(), object.build());
        data.put(object, object.build());
        counter++;
        logger.info("size after insert {}", data.size());
    }

    @Override
    public void delete(IndexableObject object) throws IOException {
        logger.info("delete {}", object.toString());
        data.remove(object);
        counter--;
        logger.info("size after delete {}", data.size());
    }

    public Map<IndexableObject, String> data() {
        return data;
    }

    @Override
    public MockSink setContext(MockContext context) {
        return this;
    }

    @Override
    public Sink setIndex(String index) {
        return this;
    }

    @Override
    public String getIndex() {
        return null;
    }

    @Override
    public Sink setIndexSettings(Settings indexSettings) {
        return this;
    }

    @Override
    public Sink setTypeMapping(Map<String, String> typeMapping) {
        return this;
    }

    @Override
    public Sink setType(String type) {
        return this;
    }

    @Override
    public String getType() {
        return null;
    }

    @Override
    public Sink setId(String id) {
        return this;
    }

    @Override
    public String getId() {
        return null;
    }

    @Override
    public void shutdown() throws IOException {
    }

    @Override
    public Sink setMetric(Metric metric) {
        return this;
    }

    @Override
    public Metric getMetric() {
        return null;
    }

    // for tests
    public long getCounter() {
        return counter;
    }

}
