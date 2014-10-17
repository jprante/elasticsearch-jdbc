package org.xbib.elasticsearch.river.jdbc.support;

import org.testng.Assert;
import org.testng.annotations.Test;
import org.xbib.elasticsearch.plugin.jdbc.util.RiverMouthKeyValueStreamListener;
import org.xbib.elasticsearch.river.jdbc.strategy.mock.MockRiverMouth;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;

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
                "{[null/null/null/1]->{person={salary=\"$1000\", name=\"Joe Doe\", position={name=\"Worker\", since=\"2012-06-12\"}}}={\"person\":{\"salary\":\"$1000\",\"name\":\"Joe Doe\",\"position\":{\"name\":\"Worker\",\"since\":\"2012-06-12\"}}}, [null/null/null/2]->{person={salary=\"$2000\", name=\"Bill Smith\", position={name=\"Boss\", since=\"2012-06-13\"}}}={\"person\":{\"salary\":\"$2000\",\"name\":\"Bill Smith\",\"position\":{\"name\":\"Boss\",\"since\":\"2012-06-13\"}}}}"
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
        assertEquals(output.data().toString(),
                "{[null/null/null/1]->{person={salary=\"$1000\", name=\"Joe Doe\", position={name=\"Worker\", since=[\"2012-06-12\",\"2012-06-13\"]}}}={\"person\":{\"salary\":\"$1000\",\"name\":\"Joe Doe\",\"position\":{\"name\":\"Worker\",\"since\":[\"2012-06-12\",\"2012-06-13\"]}}}}"
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
                "{[null/null/null/1]->{person={salary=\"$1000\", name=\"Joe Doe\", position={name=\"Worker\", since=\"2012-06-13\"}}}={\"person\":{\"salary\":\"$1000\",\"name\":\"Joe Doe\",\"position\":{\"name\":\"Worker\",\"since\":\"2012-06-13\"}}}}"
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
                "{[null/null/null/1]->{person={salary=\"$1000\", name=[\"Joe\",\"John\",\"Mark\"], position={name=\"Worker\", since=\"2012-06-13\"}}}={\"person\":{\"salary\":\"$1000\",\"name\":[\"Joe\",\"John\",\"Mark\"],\"position\":{\"name\":\"Worker\",\"since\":\"2012-06-13\"}}}, [null/null/null/2]->{person={salary=\"$1000\", name=\"Mark\", position={name=\"Worker\", since=\"2012-06-13\"}}}={\"person\":{\"salary\":\"$1000\",\"name\":\"Mark\",\"position\":{\"name\":\"Worker\",\"since\":\"2012-06-13\"}}}}"
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
                "{[null/null/null/1]->{person={name=\"Andrew Ng\", coursename=[{name=\"Machine Learning\", count=\"5\"}, {name=\"Recommender Systems\", count=\"5\"}]}}={\"person\":{\"name\":\"Andrew Ng\",\"coursename\":[{\"name\":\"Machine Learning\",\"count\":\"5\"},{\"name\":\"Recommender Systems\",\"count\":\"5\"}]}}, [null/null/null/2]->{person={name=\"Doug Cutting\", coursename=[{name=\"Hadoop Internals\", count=\"12\"}, {name=\"Basic of Lucene\", count=\"25\"}, {name=\"Advanced Lucene\", count=\"5\"}, {name=\"Introduction to Apache Avro\", count=\"5\"}]}}={\"person\":{\"name\":\"Doug Cutting\",\"coursename\":[{\"name\":\"Hadoop Internals\",\"count\":\"12\"},{\"name\":\"Basic of Lucene\",\"count\":\"25\"},{\"name\":\"Advanced Lucene\",\"count\":\"5\"},{\"name\":\"Introduction to Apache Avro\",\"count\":\"5\"}]}}}"
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
                "{[null/null/null/4679]->{blog={name=\"Tesla, Abe and Elba\", published=\"2014-01-06 00:00:00\", association=[{id=\"3917\", name=\"Idris Elba\"}, {id=\"3917\", name=\"Idris Elba\"}, {id=\"3917\", name=\"Idris Elba\"}], attachment=[{id=\"9450\", name=\"/web/q/g/h/57436356.jpg\"}, {id=\"9965\", name=\"/web/i/s/q/GS3193626.jpg\"}, {id=\"9451\", name=\"/web/i/s/q/GS3193626.jpg\"}]}}={\"blog\":{\"name\":\"Tesla, Abe and Elba\",\"published\":\"2014-01-06 00:00:00\",\"association\":[{\"id\":\"3917\",\"name\":\"Idris Elba\"},{\"id\":\"3917\",\"name\":\"Idris Elba\"},{\"id\":\"3917\",\"name\":\"Idris Elba\"}],\"attachment\":[{\"id\":\"9450\",\"name\":\"/web/q/g/h/57436356.jpg\"},{\"id\":\"9965\",\"name\":\"/web/i/s/q/GS3193626.jpg\"},{\"id\":\"9451\",\"name\":\"/web/i/s/q/GS3193626.jpg\"}]}}}"
        );
    }

    @Test
    public void testDoubleScientificNotation() throws Exception {
        Locale.setDefault(Locale.US);
        List<String> columns = Arrays.asList("_id", "lat", "lon");
        List<Object> row1 = new LinkedList<Object>();
        row1.add("1");
        row1.add(50.940664);
        row1.add(6.9599115);
        MockRiverMouth output = new MockRiverMouth();
        new RiverMouthKeyValueStreamListener<String, Object>()
                .output(output)
                .begin()
                .keys(columns)
                .values(row1)
                .end();
        assertEquals(output.data().toString(),
                "{[null/null/null/1]->{lat=50.940664000000, lon=6.959911500000}={\"lat\":50.940664,\"lon\":6.9599115}}"
        );
    }

    @Test
    public void testNestedDots() throws Exception {
        List<String> columns = Arrays.asList("_id", "person.name", "person.coursename[teacher.id]", "person.coursename[teacher.name]");
        List<String> row1 = Arrays.asList("1", "Andrew Ng", "1", "Brian Smith");
        List<String> row2 = Arrays.asList("1", "Andrew Ng", "2", "Marc Levengood");
        List<String> row3 = Arrays.asList("2", "Doug Cutting", "1", "Brian Smith");
        List<String> row4 = Arrays.asList("2", "Doug Cutting", "2", "Marc Levengood");
        List<String> row5 = Arrays.asList("2", "Doug Cutting", "3", "Samantha Carter");
        List<String> row6 = Arrays.asList("2", "Doug Cutting", "4", "Jack O'Neill");
        MockRiverMouth output = new MockRiverMouth();
        new RiverMouthKeyValueStreamListener<String, String>()
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
                "{[null/null/null/1]->{person={name=\"Andrew Ng\", coursename=[{teacher={id=\"1\", name=\"Brian Smith\"}}, {teacher={id=\"2\", name=\"Marc Levengood\"}}]}}={\"person\":{\"name\":\"Andrew Ng\",\"coursename\":[{\"teacher\":{\"id\":\"1\",\"name\":\"Brian Smith\"}},{\"teacher\":{\"id\":\"2\",\"name\":\"Marc Levengood\"}}]}}, [null/null/null/2]->{person={name=\"Doug Cutting\", coursename=[{teacher={id=\"1\", name=\"Brian Smith\"}}, {teacher={id=\"2\", name=\"Marc Levengood\"}}, {teacher={id=\"3\", name=\"Samantha Carter\"}}, {teacher={id=\"4\", name=\"Jack O'Neill\"}}]}}={\"person\":{\"name\":\"Doug Cutting\",\"coursename\":[{\"teacher\":{\"id\":\"1\",\"name\":\"Brian Smith\"}},{\"teacher\":{\"id\":\"2\",\"name\":\"Marc Levengood\"}},{\"teacher\":{\"id\":\"3\",\"name\":\"Samantha Carter\"}},{\"teacher\":{\"id\":\"4\",\"name\":\"Jack O'Neill\"}}]}}}"
        );
    }

    @Test
    public void testIgnoreNull() throws Exception {
        List<String> columns = Arrays.asList("_id", "col1", "col2");
        List<Object> row1 = new LinkedList<Object>();
        row1.add("1");
        row1.add("Hello World");
        row1.add(null);
        List<Object> row2 = new LinkedList<Object>();
        row2.add("1");
        row2.add(null);
        row2.add("Hello World");
        MockRiverMouth output = new MockRiverMouth();
        new RiverMouthKeyValueStreamListener<String, Object>()
                .shouldIgnoreNull(true)
                .output(output)
                .begin()
                .keys(columns)
                .values(row1)
                .values(row2)
                .end();
        assertEquals(output.data().toString(),
                "{[null/null/null/1]->{col1=\"Hello World\", col2=\"Hello World\"}={\"col1\":\"Hello World\",\"col2\":\"Hello World\"}}"
        );
    }

    @Test
    public void testIgnoreNullObject() throws Exception {
        List<String> columns = Arrays.asList("_id", "blog.name", "blog.association[id]", "blog.association[name]");
        List<Object> row1 = new LinkedList<Object>();
        row1.add("4679");
        row1.add("Joe");
        row1.add("3917");
        row1.add("John");
        List<Object> row2 = new LinkedList<Object>();
        row2.add("4679");
        row2.add("Joe");
        row2.add("4015");
        row2.add("Jack");
        List<Object> row3 = new LinkedList<Object>();
        row3.add("4679");
        row3.add("Joe");
        row3.add(null);
        row3.add(null);
        MockRiverMouth output = new MockRiverMouth();
        new RiverMouthKeyValueStreamListener<String, Object>()
                .shouldIgnoreNull(true)
                .output(output)
                .begin()
                .keys(columns)
                .values(row1)
                .values(row2)
                .values(row3)
                .end();
        assertEquals(output.data().toString(),
                "{[null/null/null/4679]->{blog={name=\"Joe\", association=[{id=\"3917\", name=\"John\"}, {id=\"4015\", name=\"Jack\"}, {id=null, name=null}]}}={\"blog\":{\"name\":\"Joe\",\"association\":[{\"id\":\"3917\",\"name\":\"John\"},{\"id\":\"4015\",\"name\":\"Jack\"}]}}}"
        );
    }


    @Test
    public void testIgnoreNullObject2() throws Exception {
        List<String> columns = Arrays.asList("_id", "movie.event", "movie.title", "movie.overview", "movie.test");
        List<Object> row1 = new LinkedList<Object>();
        row1.add(0);
        row1.add(null);
        row1.add(null);
        row1.add(null);
        row1.add(null);
        List<Object> row2 = new LinkedList<Object>();
        row2.add(1);
        row2.add(21);
        row2.add("ABC");
        row2.add("DEF");
        row2.add(1212);
        MockRiverMouth output = new MockRiverMouth();
        new RiverMouthKeyValueStreamListener<String, Object>()
                .shouldIgnoreNull(true)
                .output(output)
                .begin()
                .keys(columns)
                .values(row1)
                .values(row2)
                .end();

        assertEquals(output.data().toString(),
                "{[null/null/null/0]->{movie={event=null, title=null, overview=null, test=null}}={\"movie\":{}}, [null/null/null/1]->{movie={event=21, title=\"ABC\", overview=\"DEF\", test=1212}}={\"movie\":{\"event\":21,\"title\":\"ABC\",\"overview\":\"DEF\",\"test\":1212}}}"
        );

    }

}
