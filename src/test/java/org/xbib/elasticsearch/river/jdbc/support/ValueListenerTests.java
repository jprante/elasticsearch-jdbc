package org.xbib.elasticsearch.river.jdbc.support;

import org.testng.Assert;
import org.testng.annotations.Test;
import org.xbib.elasticsearch.plugin.jdbc.RiverMouthKeyValueStreamListener;
import org.xbib.elasticsearch.river.jdbc.strategy.mock.MockRiverMouth;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class ValueListenerTests extends Assert {

    @Test
    public void testId() throws Exception {
        List<String> columns = Arrays.asList("_optype", "_id", "label");
        List<String> row1 = Arrays.asList("index", "1", "label1");
        List<String> row2 = Arrays.asList("index", "2", "label2");
        List<String> row3 = Arrays.asList("index", "3", "label3");
        List<String> row4 = Arrays.asList("index", "4", "label4");
        MockRiverMouth output = new MockRiverMouth();
        new StringKeyValueStreamListener()
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
        new StringKeyValueStreamListener()
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
        new StringKeyValueStreamListener()
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
        new StringKeyValueStreamListener()
                .output(output)
                .begin()
                .keys(columns)
                .values(row1)
                .values(row2)
                .end();
        assertEquals(output.data().size(), 2, "Number of inserted objects");
        assertEquals(output.data().toString(),
                "{[null/null/null/1]->{person={name=\"Joe Doe\", position={name=\"Worker\", since=\"2012-06-12\"}, salary=\"$1000\"}}={\"person\":{\"name\":\"Joe Doe\",\"position\":{\"name\":\"Worker\",\"since\":\"2012-06-12\"},\"salary\":\"$1000\"}}, [null/null/null/2]->{person={name=\"Bill Smith\", position={name=\"Boss\", since=\"2012-06-13\"}, salary=\"$2000\"}}={\"person\":{\"name\":\"Bill Smith\",\"position\":{\"name\":\"Boss\",\"since\":\"2012-06-13\"},\"salary\":\"$2000\"}}}"
        );
    }

    @Test
    public void testMultipleValues() throws Exception {
        List<String> columns = Arrays.asList("_id", "person.salary", "person.name", "person.position.name", "person.position.since");
        List<String> row1 = Arrays.asList("1", "$1000", "Joe Doe", "Worker", "2012-06-12");
        List<String> row2 = Arrays.asList("1", "$1000", "Joe Doe", "Worker", "2012-06-13");
        MockRiverMouth output = new MockRiverMouth();
        new StringKeyValueStreamListener()
                .output(output)
                .begin()
                .keys(columns)
                .values(row1)
                .values(row2)
                .end();
        //assertEquals(output.data().size(), 1, "Number of inserted objects");
        assertEquals(output.data().toString(),
                "{[null/null/null/1]->{person={name=\"Joe Doe\", position={name=\"Worker\", since=[\"2012-06-12\",\"2012-06-13\"]}, salary=\"$1000\"}}={\"person\":{\"name\":\"Joe Doe\",\"position\":{\"name\":\"Worker\",\"since\":[\"2012-06-12\",\"2012-06-13\"]},\"salary\":\"$1000\"}}}"
        );
    }

    @Test
    public void testMultipleValuesWithNull() throws Exception {
        List<String> columns = Arrays.asList("_id", "person.salary", "person.name", "person.position.name", "person.position.since");
        List<String> row1 = Arrays.asList("1", "$1000", "Joe Doe", "Worker", null);
        List<String> row2 = Arrays.asList("1", "$1000", "Joe Doe", "Worker", "2012-06-13");
        MockRiverMouth output = new MockRiverMouth();
        new StringKeyValueStreamListener()
                .output(output)
                .begin()
                .keys(columns)
                .values(row1)
                .values(row2)
                .end();
        assertEquals(output.data().size(), 1, "Number of inserted objects");
        assertEquals(output.data().toString(),
                "{[null/null/null/1]->{person={name=\"Joe Doe\", position={name=\"Worker\", since=\"2012-06-13\"}, salary=\"$1000\"}}={\"person\":{\"name\":\"Joe Doe\",\"position\":{\"name\":\"Worker\",\"since\":\"2012-06-13\"},\"salary\":\"$1000\"}}}"
        );
    }

    @Test
    public void testSequenceValues() throws Exception {
        List<String> columns = Arrays.asList("_id", "person.salary", "person.name[]", "person.position.name", "person.position.since");
        List<String> row1 = Arrays.asList("1", "$1000", "Joe,John", "Worker", null);
        List<String> row2 = Arrays.asList("1", "$1000", "Mark", "Worker", "2012-06-13");
        List<String> row3 = Arrays.asList("2", "$1000", "Mark", "Worker", "2012-06-13");
        MockRiverMouth output = new MockRiverMouth();
        new StringKeyValueStreamListener()
                .output(output)
                .begin()
                .keys(columns)
                .values(row1)
                .values(row2)
                .values(row3)
                .end();
        assertEquals(output.data().size(), 2, "Number of inserted objects");
        assertEquals(output.data().toString(),
                "{[null/null/null/1]->{person={name=[\"Joe\",\"John\",\"Mark\"], position={name=\"Worker\", since=\"2012-06-13\"}, salary=\"$1000\"}}={\"person\":{\"name\":[\"Joe\",\"John\",\"Mark\"],\"position\":{\"name\":\"Worker\",\"since\":\"2012-06-13\"},\"salary\":\"$1000\"}}, [null/null/null/2]->{person={name=\"Mark\", position={name=\"Worker\", since=\"2012-06-13\"}, salary=\"$1000\"}}={\"person\":{\"name\":\"Mark\",\"position\":{\"name\":\"Worker\",\"since\":\"2012-06-13\"},\"salary\":\"$1000\"}}}"
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
        new StringKeyValueStreamListener()
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
                "{[null/null/null/1]->{person={coursename=[{name=\"Machine Learning\", count=\"5\"}, {name=\"Recommender Systems\", count=\"5\"}], name=\"Andrew Ng\"}}={\"person\":{\"coursename\":[{\"name\":\"Machine Learning\",\"count\":\"5\"},{\"name\":\"Recommender Systems\",\"count\":\"5\"}],\"name\":\"Andrew Ng\"}}, [null/null/null/2]->{person={coursename=[{name=\"Hadoop Internals\", count=\"12\"}, {name=\"Basic of Lucene\", count=\"25\"}, {name=\"Advanced Lucene\", count=\"5\"}, {name=\"Introduction to Apache Avro\", count=\"5\"}], name=\"Doug Cutting\"}}={\"person\":{\"coursename\":[{\"name\":\"Hadoop Internals\",\"count\":\"12\"},{\"name\":\"Basic of Lucene\",\"count\":\"25\"},{\"name\":\"Advanced Lucene\",\"count\":\"5\"},{\"name\":\"Introduction to Apache Avro\",\"count\":\"5\"}],\"name\":\"Doug Cutting\"}}}"
        );
    }

    @Test
    public void testJSONSource() throws Exception {
        List<String> columns = Arrays.asList("_optype", "_id", "_source");
        List<String> row1 = Arrays.asList("index", "1", "{\"Hello\":\"World\"}");
        MockRiverMouth output = new MockRiverMouth();
        new StringKeyValueStreamListener()
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
        new StringKeyValueStreamListener()
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
        new StringKeyValueStreamListener()
                .output(output)
                .begin()
                .keys(columns)
                .values(row1)
                .values(row2)
                .end();
        assertEquals(output.data().toString(),
                "{[index/null/null/1]->{message=\"{Hello=World}\", person=[\"{name=[Joe, John], attributes=\"{haircolor=blue}\"}\",\"{name=[Joe, John]}\"]}={\"message\":{\"Hello\":\"World\"},\"person\":[{\"name\":[\"Joe\",\"John\"],\"attributes\":{\"haircolor\":\"blue\"}},{\"name\":[\"Joe\",\"John\"]}]}}"
        );
    }

    @Test
    public void testArrays() throws Exception {
        List<String> columns = Arrays.asList("_id", "blog.name", "blog.published", "blog.association[id]", "blog.association[name]", "blog.attachment[id]", "blog.attachment[name]");
        List<String> row1 = Arrays.asList("4679", "Tesla, Abe and Elba", "2014-01-06 00:00:00", "3917", "Idris Elba", "9450", "/web/q/g/h/57436356.jpg");
        List<String> row2 = Arrays.asList("4679", "Tesla, Abe and Elba", "2014-01-06 00:00:00", "3917", "Idris Elba", "9965", "/web/i/s/q/GS3193626.jpg");
        List<String> row3 = Arrays.asList("4679", "Tesla, Abe and Elba", "2014-01-06 00:00:00", "3917", "Idris Elba", "9451", "/web/i/s/q/GS3193626.jpg");
        MockRiverMouth output = new MockRiverMouth();
        new StringKeyValueStreamListener()
                .output(output)
                .begin()
                .keys(columns)
                .values(row1)
                .values(row2)
                .values(row3)
                .end();
        assertEquals(output.data().toString(),
                "{[null/null/null/4679]->{blog={attachment=[{name=\"/web/q/g/h/57436356.jpg\", id=\"9450\"}, {name=\"/web/i/s/q/GS3193626.jpg\", id=\"9965\"}, {name=\"/web/i/s/q/GS3193626.jpg\", id=\"9451\"}], name=\"Tesla, Abe and Elba\", association=[{name=\"Idris Elba\", id=\"3917\"}, {name=\"Idris Elba\", id=\"3917\"}, {name=\"Idris Elba\", id=\"3917\"}], published=\"2014-01-06 00:00:00\"}}={\"blog\":{\"attachment\":[{\"name\":\"/web/q/g/h/57436356.jpg\",\"id\":\"9450\"},{\"name\":\"/web/i/s/q/GS3193626.jpg\",\"id\":\"9965\"},{\"name\":\"/web/i/s/q/GS3193626.jpg\",\"id\":\"9451\"}],\"name\":\"Tesla, Abe and Elba\",\"association\":[{\"name\":\"Idris Elba\",\"id\":\"3917\"},{\"name\":\"Idris Elba\",\"id\":\"3917\"},{\"name\":\"Idris Elba\",\"id\":\"3917\"}],\"published\":\"2014-01-06 00:00:00\"}}}"
        );
    }

    @Test
    public void testDoubleScientificNotation() throws Exception {
        List<String> columns = Arrays.asList("_id", "lat", "lon");
        List<Object> row1 = new LinkedList<Object>();
        row1.add("1");
        row1.add(50.940664);
        row1.add(6.9599115);
        MockRiverMouth output = new MockRiverMouth();
        new RiverMouthKeyValueStreamListener<String,Object>()
                .output(output)
                .begin()
                .keys(columns)
                .values(row1)
                .end();
        assertEquals(output.data().toString(),
                "{[null/null/null/1]->{lat=50,940664000000, lon=6,959911500000}={\"lat\":50.940664,\"lon\":6.9599115}}"
        );
    }

}
