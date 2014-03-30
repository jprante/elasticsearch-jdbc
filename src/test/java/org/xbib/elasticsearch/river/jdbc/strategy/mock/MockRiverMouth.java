
package org.xbib.elasticsearch.river.jdbc.strategy.mock;

import java.io.IOException;
import java.util.Map;

import org.elasticsearch.client.Client;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.xbib.elasticsearch.river.jdbc.RiverMouth;
import org.xbib.elasticsearch.river.jdbc.support.RiverContext;
import org.xbib.elasticsearch.gatherer.IndexableObject;

import static org.elasticsearch.common.collect.Maps.newTreeMap;

public class MockRiverMouth implements RiverMouth {

    private static final ESLogger logger = ESLoggerFactory.getLogger(MockRiverMouth.class.getName());

    private Map<String, String> data;

    private long counter;

    @Override
    public String strategy() {
        return "mock";
    }

    public MockRiverMouth() {
        data = newTreeMap(); // sort order for stability in assertions
        counter = 0L;
    }

    @Override
    public void index(IndexableObject object, boolean create) throws IOException {
        data.put(object.toString(), object.build());
        counter++;
    }

    @Override
    public void delete(IndexableObject object) throws IOException {
        data.remove(object.toString());
        counter--;
    }

    public Map<String, String> data() {
        return data;
    }

    @Override
    public RiverMouth riverContext(RiverContext context) {
        return this;
    }

    @Override
    public RiverMouth client(Client client) {
        return this;
    }

    @Override
    public Client client() {
        return null;
    }

    @Override
    public RiverMouth setSettings(Map<String,Object> settings) {
        return this;
    }

    @Override
    public RiverMouth setMapping(Map<String,Object> mapping) {
        return this;
    }

    @Override
    public RiverMouth setIndex(String index) {
        return this;
    }

    @Override
    public String getIndex() {
        return null;
    }

    @Override
    public RiverMouth setType(String type) {
        return this;
    }

    @Override
    public String getType() {
        return null;
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
    public RiverMouth setMaxBulkActions(int actions) {
        return this;
    }

    @Override
    public RiverMouth setMaxConcurrentBulkRequests(int max) {
        return this;
    }

    @Override
    public RiverMouth setMaxVolumePerBulkRequest(ByteSizeValue maxVolumePerBulkRequest) {
        return this;
    }

    @Override
    public RiverMouth setFlushInterval(TimeValue flushInterval) {
        return this;
    }

    @Override
    public void flush() throws IOException {
    }

    @Override
    public void close() {
    }

    @Override
    public void waitForCluster() throws IOException {
    }

    public long getCounter() {
        return counter;
    }

}
