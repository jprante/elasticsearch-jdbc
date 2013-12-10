/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.xbib.elasticsearch.river.jdbc.strategy.mock;

import org.elasticsearch.client.Client;

import org.xbib.elasticsearch.river.jdbc.RiverMouth;
import org.xbib.elasticsearch.river.jdbc.support.RiverContext;
import org.xbib.elasticsearch.river.jdbc.support.StructuredObject;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class MockRiverMouth implements RiverMouth {

    private Map<String, String> data;

    private long counter;

    @Override
    public String strategy() {
        return "mock";
    }

    public MockRiverMouth() {
        data = new TreeMap(); // sort order for stability in assertions
        counter = 0L;
    }

    @Override
    public void create(StructuredObject object) throws IOException {
        data.put(object.toString(), object.build());
        //logger.debug("got data for creation: {}", data);
        counter++;
    }

    @Override
    public void index(StructuredObject object) throws IOException {
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
    public RiverMouth index(String index) {
        return this;
    }

    @Override
    public String index() {
        return null;
    }

    @Override
    public RiverMouth type(String type) {
        return this;
    }

    @Override
    public String type() {
        return null;
    }

    @Override
    public RiverMouth id(String id) {
        return this;
    }

    @Override
    public RiverMouth maxBulkActions(int actions) {
        return this;
    }

    @Override
    public int maxBulkActions() {
        return 100;
    }

    @Override
    public RiverMouth maxConcurrentBulkRequests(int max) {
        return this;
    }

    @Override
    public int maxConcurrentBulkRequests() {
        return 1;
    }

    @Override
    public RiverMouth acknowledge(boolean enable) {
        return this;
    }

    @Override
    public boolean acknowledge() {
        return false;
    }

    @Override
    public void flush() throws IOException {
    }

    @Override
    public void close() {
    }

    @Override
    public void createIndexIfNotExists(String settings, String mapping) {
    }

    @Override
    public void waitForCluster() throws IOException {
    }

    public long getCounter() {
        return counter;
    }

}
