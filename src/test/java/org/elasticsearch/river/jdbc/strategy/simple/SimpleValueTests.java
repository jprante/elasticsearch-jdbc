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
package org.elasticsearch.river.jdbc.strategy.simple;

import org.elasticsearch.river.jdbc.strategy.mock.MockRiverMouth;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

public class SimpleValueTests extends Assert {

    @Test
    public void testId() throws Exception {
        List<String> columns = Arrays.asList("_optype", "_id", "label");
        List<String> row1 = Arrays.asList("index", "1", "label1");
        List<String> row2 = Arrays.asList("index", "2", "label2");
        List<String> row3 = Arrays.asList("index", "3", "label3");
        List<String> row4 = Arrays.asList("index", "4", "label4");
        MockRiverMouth target = new MockRiverMouth();
        new SimpleValueListener()
                .target(target)
                .begin()
                .keys(columns)
                .values(row1)
                .values(row2)
                .values(row3)
                .values(row4)
                .end();
        assertEquals(target.data().size(), 4, "Number of inserted objects");
    }

    @Test
    public void testOptype() throws Exception {
        List<String> columns = Arrays.asList("_optype", "_id", "label");
        List<String> row1 = Arrays.asList("index", "1", "label1");
        List<String> row2 = Arrays.asList("create", "2", "label2");
        List<String> row3 = Arrays.asList("delete", "3", "label3");
        List<String> row4 = Arrays.asList("index", "4", "label4");
        MockRiverMouth target = new MockRiverMouth();
        new SimpleValueListener()
                .target(target)
                .begin()
                .keys(columns)
                .values(row1)
                .values(row2)
                .values(row3)
                .values(row4)
                .end();
        assertEquals(target.data().size(), 3, "number of objects");
    }

    @Test
    public void testOverlappingOps() throws Exception {
        List<String> columns = Arrays.asList("_optype", "_id", "label");
        List<String> row1 = Arrays.asList("index", "1", "label1");
        List<String> row2 = Arrays.asList("create", "1", "label2");
        List<String> row3 = Arrays.asList("delete", "2", "label3");
        List<String> row4 = Arrays.asList("index", "2", "label4");
        MockRiverMouth target = new MockRiverMouth();
        new SimpleValueListener()
                .target(target)
                .begin()
                .keys(columns)
                .values(row1)
                .values(row2)
                .values(row3)
                .values(row4)
                .end();
        assertEquals(target.data().size(), 3, "Number of objects");
    }

    @Test
    public void testDigest() throws Exception {
        List<String> columns = Arrays.asList("_optype", "_id", "label");
        List<String> row1 = Arrays.asList("create", "1", "digest1");
        List<String> row2 = Arrays.asList("index", "1", "digest1");
        List<String> row3 = Arrays.asList("delete", "2", "digest3");
        List<String> row4 = Arrays.asList("index", "2", "digest4");
        MockRiverMouth target = new MockRiverMouth();
        new SimpleValueListener()
                .digest(true)
                .target(target)
                .begin()
                .keys(columns)
                .values(row1)
                .values(row2)
                .values(row3)
                .values(row4)
                .end();
        assertEquals(target.data().size(), 3, "Number of objects");
    }

    @Test
    public void testNestedObjects() throws Exception {
        List<String> columns = Arrays.asList("_id", "person.salary", "person.name", "person.position.name", "person.position.since");
        List<String> row1 = Arrays.asList("1", "$1000", "Joe Doe", "Worker", "2012-06-12");
        List<String> row2 = Arrays.asList("2", "$2000", "Bill Smith", "Boss", "2012-06-13");
        MockRiverMouth target = new MockRiverMouth();
        new SimpleValueListener()
                .target(target)
                .begin()
                .keys(columns)
                .values(row1)
                .values(row2)
                .end();
        assertEquals(target.data().size(), 2,
                "Number of inserted objects");
        assertEquals(target.data().toString(),
                "{null/null/null/1 {person={position={name=\"Worker\", since=\"2012-06-12\"}, name=\"Joe Doe\", salary=\"$1000\"}, _id=\"1\"}={\"person\":{\"position\":{\"name\":\"Worker\",\"since\":\"2012-06-12\"},\"name\":\"Joe Doe\",\"salary\":\"$1000\"},\"_id\":\"1\"}, null/null/null/2 {person={position={name=\"Boss\", since=\"2012-06-13\"}, name=\"Bill Smith\", salary=\"$2000\"}, _id=\"2\"}={\"person\":{\"position\":{\"name\":\"Boss\",\"since\":\"2012-06-13\"},\"name\":\"Bill Smith\",\"salary\":\"$2000\"},\"_id\":\"2\"}}");
    }

    @Test
    public void testMultipleValues() throws Exception {
        List<String> columns = Arrays.asList("_id", "person.salary", "person.name", "person.position.name", "person.position.since");
        List<String> row1 = Arrays.asList("1", "$1000", "Joe Doe", "Worker", "2012-06-12");
        List<String> row2 = Arrays.asList("1", "$1000", "Joe Doe", "Worker", "2012-06-13");
        MockRiverMouth target = new MockRiverMouth();
        new SimpleValueListener()
            .target(target)
            .begin()
            .keys(columns)
            .values(row1)
            .values(row2)
            .end();
        assertEquals(target.data().size(), 1, "Number of inserted objects");
        assertEquals(target.data().toString(),
            "{null/null/null/1 {person={position={name=\"Worker\", since=[\"2012-06-12\",\"2012-06-13\"]}, name=\"Joe Doe\", salary=\"$1000\"}, _id=\"1\"}={\"person\":{\"position\":{\"name\":\"Worker\",\"since\":[\"2012-06-12\",\"2012-06-13\"]},\"name\":\"Joe Doe\",\"salary\":\"$1000\"},\"_id\":\"1\"}}");
    }

    @Test
    public void testMultipleValuesWithNull() throws Exception {
        List<String> columns = Arrays.asList("_id", "person.salary", "person.name", "person.position.name", "person.position.since");
        List<String> row1 = Arrays.asList("1", "$1000", "Joe Doe", "Worker", null);
        List<String> row2 = Arrays.asList("1", "$1000", "Joe Doe", "Worker", "2012-06-13");
        MockRiverMouth target = new MockRiverMouth();
        new SimpleValueListener()
            .target(target)
            .begin()
            .keys(columns)
            .values(row1)
            .values(row2)
            .end();
        assertEquals(target.data().size(), 1, "Number of inserted objects");
        assertEquals(target.data().toString(),
            "{null/null/null/1 {person={position={name=\"Worker\", since=\"2012-06-13\"}, name=\"Joe Doe\", salary=\"$1000\"}, _id=\"1\"}={\"person\":{\"position\":{\"name\":\"Worker\",\"since\":\"2012-06-13\"},\"name\":\"Joe Doe\",\"salary\":\"$1000\"},\"_id\":\"1\"}}");
    }

    @Test
    public void testExpandValues() throws Exception {
        List<String> columns = Arrays.asList("_id", "person.salary", "person.name[]", "person.position.name", "person.position.since");
        List<String> row1 = Arrays.asList("1", "$1000", "Joe,John", "Worker", null);
        List<String> row2 = Arrays.asList("1", "$1000", "Mark", "Worker", "2012-06-13");
        List<String> row3 = Arrays.asList("2", "$1000", "Mark", "Worker", "2012-06-13");
        MockRiverMouth target = new MockRiverMouth();
        new SimpleValueListener()
            .target(target)
            .begin()
            .keys(columns)
            .values(row1)
            .values(row2)
            .values(row3)
            .end();
        assertEquals(target.data().size(), 2, "Number of inserted objects");
        assertEquals(target.data().toString(),
            "{null/null/null/1 {person={position={name=\"Worker\", since=\"2012-06-13\"}, name=[\"Joe\",\"John\",\"Mark\"], salary=\"$1000\"}, _id=\"1\"}={\"person\":{\"position\":{\"name\":\"Worker\",\"since\":\"2012-06-13\"},\"name\":[\"Joe\",\"John\",\"Mark\"],\"salary\":\"$1000\"},\"_id\":\"1\"}, null/null/null/2 {person={position={name=\"Worker\", since=\"2012-06-13\"}, name=\"Mark\", salary=\"$1000\"}, _id=\"2\"}={\"person\":{\"position\":{\"name\":\"Worker\",\"since\":\"2012-06-13\"},\"name\":\"Mark\",\"salary\":\"$1000\"},\"_id\":\"2\"}}");
    }

}
