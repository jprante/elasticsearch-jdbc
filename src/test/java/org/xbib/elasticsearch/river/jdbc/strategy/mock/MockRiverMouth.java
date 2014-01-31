
package org.xbib.elasticsearch.river.jdbc.strategy.mock;

import java.io.IOException;
import java.util.Map;

import org.elasticsearch.client.Client;

import org.elasticsearch.common.unit.TimeValue;
import org.xbib.elasticsearch.river.jdbc.RiverMouth;
import org.xbib.elasticsearch.river.jdbc.support.RiverContext;
import org.xbib.elasticsearch.river.jdbc.support.StructuredObject;

import static org.elasticsearch.common.collect.Maps.newTreeMap;

public class MockRiverMouth implements RiverMouth {

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
    public void index(StructuredObject object, boolean create) throws IOException {
        data.put(object.toString(), object.build());
        //logger.debug("got data for index: {}", data);
        counter++;
    }

    @Override
    public void delete(StructuredObject object) throws IOException {
        data.remove(object.toString());
        //logger.debug("got data for delete: {}", data);
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
