package org.xbib.elasticsearch.jdbc.strategy.mock;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.xbib.elasticsearch.common.client.IngestFactory;
import org.xbib.elasticsearch.common.client.Metric;
import org.xbib.elasticsearch.common.util.IndexableObject;
import org.xbib.elasticsearch.jdbc.strategy.Mouth;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class MockMouth implements Mouth<MockContext> {

    private final static ESLogger logger = ESLoggerFactory.getLogger(MockMouth.class.getSimpleName());

    private Map<IndexableObject, String> data;

    private long counter;

    @Override
    public String strategy() {
        return "mock";
    }

    @Override
    public Mouth<MockContext> newInstance() {
        return new MockMouth();
    }

    @Override
    public void beforeFetch() throws Exception {
    }

    @Override
    public void afterFetch() throws Exception {

    }

    public MockMouth() {
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
    public MockMouth setContext(MockContext context) {
        return this;
    }

    @Override
    public Mouth setIngestFactory(IngestFactory ingestFactory) {
        return this;
    }

    @Override
    public Mouth setIndex(String index) {
        return this;
    }

    @Override
    public String getIndex() {
        return null;
    }

    @Override
    public Mouth setIndexSettings(Settings indexSettings) {
        return this;
    }

    @Override
    public Mouth setTypeMapping(Map<String, String> typeMapping) {
        return this;
    }

    @Override
    public Mouth setType(String type) {
        return this;
    }

    @Override
    public String getType() {
        return null;
    }

    @Override
    public Mouth setId(String id) {
        return this;
    }

    @Override
    public String getId() {
        return null;
    }

    @Override
    public void flush() throws IOException {
    }

    @Override
    public void shutdown() throws IOException {
    }

    @Override
    public void suspend() throws Exception {
    }

    @Override
    public void resume() throws Exception {
    }

    @Override
    public Metric getMetric() {
        return null;
    }

    public long getCounter() {
        return counter;
    }

}
