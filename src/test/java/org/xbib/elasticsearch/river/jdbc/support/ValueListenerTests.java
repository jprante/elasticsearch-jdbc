
package org.xbib.elasticsearch.river.jdbc.support;

import java.util.Arrays;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import org.xbib.elasticsearch.river.jdbc.strategy.mock.MockRiverMouth;

public class ValueListenerTests extends Assert {

    @Test
    public void testId() throws Exception {
        List<String> columns = Arrays.asList("_optype", "_id", "label");
        List<String> row1 = Arrays.asList("index", "1", "label1");
        List<String> row2 = Arrays.asList("index", "2", "label2");
        List<String> row3 = Arrays.asList("index", "3", "label3");
        List<String> row4 = Arrays.asList("index", "4", "label4");
        MockRiverMouth output = new MockRiverMouth();
        new RiverKeyValueStreamListener()
                .output(output)
                .begin()
                .keys(columns)
                .values(row1)
                .values(row2)
                .values(row3)
                .values(row4)
                .end();
        assertEquals(output.data().size(), 4, "Number of inserted objects");
    }

    @Test
    public void testOptype() throws Exception {
        List<String> columns = Arrays.asList("_optype", "_id", "label");
        List<String> row1 = Arrays.asList("index", "1", "label1");
        List<String> row2 = Arrays.asList("create", "2", "label2");
        List<String> row3 = Arrays.asList("delete", "3", "label3");
        List<String> row4 = Arrays.asList("index", "4", "label4");
        MockRiverMouth output = new MockRiverMouth();
        new RiverKeyValueStreamListener()
                .output(output)
                .begin()
                .keys(columns)
                .values(row1)
                .values(row2)
                .values(row3)
                .values(row4)
                .end();
        assertEquals(output.data().size(), 3, "number of objects");
    }

    @Test
    public void testOverlappingOps() throws Exception {
        List<String> columns = Arrays.asList("_optype", "_id", "label");
        List<String> row1 = Arrays.asList("index", "1", "label1");
        List<String> row2 = Arrays.asList("create", "1", "label2");
        List<String> row3 = Arrays.asList("delete", "2", "label3");
        List<String> row4 = Arrays.asList("index", "2", "label4");
        MockRiverMouth output = new MockRiverMouth();
        new RiverKeyValueStreamListener()
                .output(output)
                .begin()
                .keys(columns)
                .values(row1)
                .values(row2)
                .values(row3)
                .values(row4)
                .end();
        assertEquals(output.data().size(), 3, "Number of objects");
    }

    @Test
    public void testNestedObjects() throws Exception {
        List<String> columns = Arrays.asList("_id", "person.salary", "person.name", "person.position.name", "person.position.since");
        List<String> row1 = Arrays.asList("1", "$1000", "Joe Doe", "Worker", "2012-06-12");
        List<String> row2 = Arrays.asList("2", "$2000", "Bill Smith", "Boss", "2012-06-13");
        MockRiverMouth output = new MockRiverMouth();
        new RiverKeyValueStreamListener()
                .output(output)
                .begin()
                .keys(columns)
                .values(row1)
                .values(row2)
                .end();
        assertEquals(output.data().size(), 2, "Number of inserted objects");
        assertEquals(output.data().toString(),
                "{[null/null/null/1]->{person={position={name=\"Worker\", since=\"2012-06-12\"}, name=\"Joe Doe\", salary=\"$1000\"}}={\"person\":{\"position\":{\"name\":\"Worker\",\"since\":\"2012-06-12\"},\"name\":\"Joe Doe\",\"salary\":\"$1000\"}}, [null/null/null/2]->{person={position={name=\"Boss\", since=\"2012-06-13\"}, name=\"Bill Smith\", salary=\"$2000\"}}={\"person\":{\"position\":{\"name\":\"Boss\",\"since\":\"2012-06-13\"},\"name\":\"Bill Smith\",\"salary\":\"$2000\"}}}");
    }

    @Test
    public void testMultipleValues() throws Exception {
        List<String> columns = Arrays.asList("_id", "person.salary", "person.name", "person.position.name", "person.position.since");
        List<String> row1 = Arrays.asList("1", "$1000", "Joe Doe", "Worker", "2012-06-12");
        List<String> row2 = Arrays.asList("1", "$1000", "Joe Doe", "Worker", "2012-06-13");
        MockRiverMouth output = new MockRiverMouth();
        new RiverKeyValueStreamListener()
            .output(output)
            .begin()
            .keys(columns)
            .values(row1)
            .values(row2)
            .end();
        assertEquals(output.data().size(), 1, "Number of inserted objects");
        assertEquals(output.data().toString(),
            "{[null/null/null/1]->{person={position={name=\"Worker\", since=[\"2012-06-12\",\"2012-06-13\"]}, name=\"Joe Doe\", salary=\"$1000\"}}={\"person\":{\"position\":{\"name\":\"Worker\",\"since\":[\"2012-06-12\",\"2012-06-13\"]},\"name\":\"Joe Doe\",\"salary\":\"$1000\"}}}");
    }

    @Test
    public void testMultipleValuesWithNull() throws Exception {
        List<String> columns = Arrays.asList("_id", "person.salary", "person.name", "person.position.name", "person.position.since");
        List<String> row1 = Arrays.asList("1", "$1000", "Joe Doe", "Worker", null);
        List<String> row2 = Arrays.asList("1", "$1000", "Joe Doe", "Worker", "2012-06-13");
        MockRiverMouth output = new MockRiverMouth();
        new RiverKeyValueStreamListener()
            .output(output)
            .begin()
            .keys(columns)
            .values(row1)
            .values(row2)
            .end();
        assertEquals(output.data().size(), 1, "Number of inserted objects");
        assertEquals(output.data().toString(),
            "{[null/null/null/1]->{person={position={name=\"Worker\", since=\"2012-06-13\"}, name=\"Joe Doe\", salary=\"$1000\"}}={\"person\":{\"position\":{\"name\":\"Worker\",\"since\":\"2012-06-13\"},\"name\":\"Joe Doe\",\"salary\":\"$1000\"}}}");
    }

    @Test
    public void testSequenceValues() throws Exception {
        List<String> columns = Arrays.asList("_id", "person.salary", "person.name[]", "person.position.name", "person.position.since");
        List<String> row1 = Arrays.asList("1", "$1000", "Joe,John", "Worker", null);
        List<String> row2 = Arrays.asList("1", "$1000", "Mark", "Worker", "2012-06-13");
        List<String> row3 = Arrays.asList("2", "$1000", "Mark", "Worker", "2012-06-13");
        MockRiverMouth output = new MockRiverMouth();
        new RiverKeyValueStreamListener()
            .output(output)
            .begin()
            .keys(columns)
            .values(row1)
            .values(row2)
            .values(row3)
            .end();
        assertEquals(output.data().size(), 2, "Number of inserted objects");
        assertEquals(output.data().toString(),
            "{[null/null/null/1]->{person={position={name=\"Worker\", since=\"2012-06-13\"}, name=[\"Joe\",\"John\",\"Mark\"], salary=\"$1000\"}}={\"person\":{\"position\":{\"name\":\"Worker\",\"since\":\"2012-06-13\"},\"name\":[\"Joe\",\"John\",\"Mark\"],\"salary\":\"$1000\"}}, [null/null/null/2]->{person={position={name=\"Worker\", since=\"2012-06-13\"}, name=\"Mark\", salary=\"$1000\"}}={\"person\":{\"position\":{\"name\":\"Worker\",\"since\":\"2012-06-13\"},\"name\":\"Mark\",\"salary\":\"$1000\"}}}"
        );
    }

    @Test
    public void testSequenceObjects() throws Exception {
        List<String> columns = Arrays.asList("_id", "person.name", "person.coursename[name]", "person.coursename[count]");
        List<String> row1 = Arrays.asList("1", "Andrew Ng", "Machine Learning", "5");
        List<String> row2 = Arrays.asList("1", "Andrew Ng", "Recommender Systems", "5");
        List<String> row3 = Arrays.asList("2", "Doug Cutting", "Hadoop Internals", "12");
        List<String> row4 = Arrays.asList("2", "Doug Cutting", "Basic of Lucene", "25");
        List<String> row5 = Arrays.asList("2", "Doug Cutting", "Advanced Lucene", "5");
        List<String> row6 = Arrays.asList("2", "Doug Cutting", "Introduction to Apache Avro", "5");
        MockRiverMouth output = new MockRiverMouth();
        new RiverKeyValueStreamListener()
                .output(output)
                .begin()
                .keys(columns)
                .values(row1)
                .values(row2)
                .values(row3)
                .values(row4)
                .values(row5)
                .values(row6)
                .end();
        assertEquals(output.data().toString(),
            "{[null/null/null/1]->{person={name=\"Andrew Ng\", coursename=[{count=\"5\", name=\"Machine Learning\"}, {count=\"5\", name=\"Recommender Systems\"}]}}={\"person\":{\"name\":\"Andrew Ng\",\"coursename\":[{\"count\":\"5\",\"name\":\"Machine Learning\"},{\"count\":\"5\",\"name\":\"Recommender Systems\"}]}}, [null/null/null/2]->{person={name=\"Doug Cutting\", coursename=[{count=\"12\", name=\"Hadoop Internals\"}, {count=\"25\", name=\"Basic of Lucene\"}, {count=\"5\", name=\"Advanced Lucene\"}, {count=\"5\", name=\"Introduction to Apache Avro\"}]}}={\"person\":{\"name\":\"Doug Cutting\",\"coursename\":[{\"count\":\"12\",\"name\":\"Hadoop Internals\"},{\"count\":\"25\",\"name\":\"Basic of Lucene\"},{\"count\":\"5\",\"name\":\"Advanced Lucene\"},{\"count\":\"5\",\"name\":\"Introduction to Apache Avro\"}]}}}"
        );
    }

    @Test
    public void testJSONSource() throws Exception {
        List<String> columns = Arrays.asList("_optype", "_id", "_source");
        List<String> row1 = Arrays.asList("index", "1", "{\"Hello\":\"World\"}");
        MockRiverMouth output = new MockRiverMouth();
        new RiverKeyValueStreamListener()
                .output(output)
                .begin()
                .keys(columns)
                .values(row1)
                .end();
        assertEquals(output.data().toString(),
                "{[index/null/null/1]->{Hello=World}={\"Hello\":\"World\"}}"
        );
    }

    @Test
    public void testJSON() throws Exception {
        List<String> columns = Arrays.asList("_optype", "_id", "message", "person", "person.attributes");
        List<String> row1 = Arrays.asList("index", "1", "{\"Hello\":\"World\"}", "{\"name\":[\"Joe\",\"John\"]}", "{\"haircolor\":\"blue\"}");
        MockRiverMouth output = new MockRiverMouth();
        new RiverKeyValueStreamListener()
                .output(output)
                .begin()
                .keys(columns)
                .values(row1)
                .end();
        assertEquals(output.data().toString(),
                "{[index/null/null/1]->{message=\"{Hello=World}\", person=\"{name=[Joe, John], attributes=\"{haircolor=blue}\"}\"}={\"message\":{\"Hello\":\"World\"},\"person\":{\"name\":[\"Joe\",\"John\"],\"attributes\":{\"haircolor\":\"blue\"}}}}"
        );
    }

    @Test
    public void testJSONWithNull() throws Exception {
        List<String> columns = Arrays.asList("_optype", "_id", "message", "person", "person.attributes");
        List<String> row1 = Arrays.asList("index", "1", "{\"Hello\":\"World\"}", "{\"name\":[\"Joe\",\"John\"]}", "{\"haircolor\":\"blue\"}");
        List<String> row2 = Arrays.asList("index", "1", null, "{\"name\":[\"Joe\",\"John\"]}", "{\"haircolor\":\"blue\"}");
        MockRiverMouth output = new MockRiverMouth();
        new RiverKeyValueStreamListener()
                .output(output)
                .begin()
                .keys(columns)
                .values(row1)
                .values(row2)
                .end();
        //assertEquals(output.data().toString(),
        //        "{[index/null/null/1]->{message=\"{Hello=World}\", person=\"{name=[Joe, John], attributes=\"{haircolor=blue}\"}\"}={\"message\":{\"Hello\":\"World\"},\"person\":{\"name\":[\"Joe\",\"John\"],\"attributes\":{\"haircolor\":\"blue\"}}}}"
        //);
    }

}
