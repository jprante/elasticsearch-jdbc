package org.xbib.elasticsearch.river.jdbc.strategy.mock;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.xbib.elasticsearch.plugin.jdbc.IndexableObject;
import org.xbib.elasticsearch.plugin.jdbc.RiverContext;
import org.xbib.elasticsearch.river.jdbc.RiverMouth;
import org.xbib.elasticsearch.support.client.Ingest;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class MockRiverMouth implements RiverMouth {

    private final static ESLogger logger = ESLoggerFactory.getLogger(MockRiverMouth.class.getName());

    private Map<IndexableObject, String> data;

    private long counter;

    @Override
    public String strategy() {
        return "mock";
    }

    public MockRiverMouth() {
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
    public RiverMouth setRiverContext(RiverContext context) {
        return this;
    }

    @Override
    public RiverMouth setIngest(Ingest ingester) {
        return this;
    }

    @Override
    public RiverMouth setIndex(String index) {
        return this;
    }
    @Override
    public RiverMouth setType(String type) {
        return this;
    }

    @Override
    public RiverMouth setTimeWindowed(boolean timeWindowed) {
        return this;
    }

    @Override
    public RiverMouth setId(String id) {
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
    public void close() {
    }

    public long getCounter() {
        return counter;
    }

}
