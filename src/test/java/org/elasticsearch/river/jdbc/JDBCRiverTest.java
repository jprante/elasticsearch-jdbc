package org.elasticsearch.river.jdbc;

import org.apache.log4j.Logger;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.log4j.Log4jESLogger;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.hsqldb.Server;
import org.hsqldb.persist.HsqlProperties;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/* Acceptance test of JDBCRiver with memory database and memory ES */
public class JDBCRiverTest {
    private final ESLogger logger = new Log4jESLogger("ES river", Logger.getLogger("Test"));
    private Server server;
    private String mandatoryQuery = "select 'index' as \"_operation\", id as _id, id as \"id\"" +
            "            from car ";

    private List<Object> mappingComplexQuery2 = ESUtilTest.createMapping("_operation","_id","id","label","options[label]","options[price]","_modification_date");
    private String complexQuery2 = "select 'index' as \"_operation\", id as \"_id\", id, label, o.label as labelopt, o.price, modification_date as \"_modification_date\"" +
            "            from car left join car_opt_have co on car.id = co.id_car" +
            "            left join opt o on o.id = co.id_opt";

    private List<Object> mappingDeleteQuery = mappingComplexQuery2;
    private String deleteQuery = "(select 'index' as \"_operation\", id as \"_id\", id, label, o.label as \"labelopt\"," +
            "            o.price , modification_date as \"_modification_date\"" +
            "            from car left join car_opt_have co on car.id = co.id_car" +
            "            left join opt o on o.id = co.id_opt where label not like '%delete%') " +
            " union (select 'delete' as \"_operation\", id as \"_id\", id,'' as \"label\", '' as \"labelopt\"," +
            "           '0', modification_date as \"_modification_date\" from car where label like'%delete%')";

    private List<Object> mappingManyJoinQuery = ESUtilTest.createMapping("_option","_id","id","label","options[label]","options[price]","colors[label]","colors[id]","modification_date");
    private String manyJoinQuery = "select 'index' as \"_operation\", id as \"_id\",id, label, o.label as \"labelopt\"," +
            "           o.price,co.label as \"labelcolor\",co.id_color,modification_date as \"_modification_date\"" +
            "           from car left join car_opt_have co on car.id = co.id_car left join opt o on o.id = co.id_opt " +
            "           left join car_color_have cc on cc.id_car = car.id left join color co on co.id_color = cc.id_color";


    private final String RIVER_INDEX_NAME = "shop";

    @BeforeClass
    public void startHsqldbServer()throws ClassNotFoundException,SQLException{
        HsqlProperties p = new HsqlProperties();
        p.setProperty("server.database.0","mem:test");
        p.setProperty("server.dbname.0","test");


        server = new Server();
        server.setProperties(p);
        server.setLogWriter(null); // can use custom writer
        server.setErrWriter(null); // can use custom writer
        server.start();

        // Insert new data
        Connection conn = SQLUtilTest.createConnection();
        conn.createStatement().execute("create table car(id int,label varchar(255),modification_date timestamp )");
        conn.createStatement().execute("create table opt(id int,label varchar(255), price int)");
        conn.createStatement().execute("create table color(id_color int,label varchar(255))");
        conn.createStatement().execute("create table car_opt_have(id_opt int, id_car int )");
        conn.createStatement().execute("create table car_color_have(id_color int, id_car int)");

        conn.createStatement().execute("insert into opt values(1,'clim',1000)");
        conn.createStatement().execute("insert into opt values(2,'door',500)");
        conn.createStatement().execute("insert into opt values(3,'wheel',200)");
        conn.createStatement().execute("insert into opt values(4,'gearsystem',350)");
        conn.createStatement().execute("insert into opt values(5,'windows',120)");

        conn.createStatement().execute("insert into color values(1,'red')");
        conn.createStatement().execute("insert into color values(2,'blue')");
        conn.createStatement().execute("insert into color values(3,'black')");
        conn.createStatement().execute("insert into color values(4,'yellow')");

        SQLUtilTest.addDataTest(conn, 1, "car1", "2012-06-02 10:00:02", new Integer[]{1, 2, 3, 4});
        SQLUtilTest.addDataTest(conn, 2, "car2", "2012-06-02 11:00:04", new Integer[]{1});
        SQLUtilTest.addDataTest(conn, 3, "car3", "2012-06-02 14:22:05", new Integer[]{1});
        SQLUtilTest.addDataTest(conn, 4, "car4", "2012-06-02 10:30:03", new Integer[]{1, 3});
        SQLUtilTest.addDataTest(conn, 5, "car5", "2012-06-02 09:00:01", new Integer[]{1});

        conn.close();
    }

    @AfterClass
    public void stopHsqldbServer()throws Exception{
        Connection conn = SQLUtilTest.createConnection();
        conn.createStatement().execute("drop table car_opt_have");
        conn.createStatement().execute("drop table opt");
        conn.createStatement().execute("drop table car");
        server.stop();
    }


    @BeforeMethod
    public void resetIndex(){
        BulkOperation op = ESUtilTest.getMemoryBulkOperation(logger).setIndex(RIVER_INDEX_NAME).setType("car");
        ESUtilTest.deleteDocumentsInIndex(op.getClient(), RIVER_INDEX_NAME);
        ESUtilTest.deleteDocumentInIndex(op.getClient(), ESUtilTest.NAME_INDEX_RIVER, ESUtilTest.TYPE_INDEX_RIVER, JDBCRiver.ID_INFO_RIVER_INDEX);
    }


    @Test
    public void testMappings(){
        BulkOperation op = ESUtilTest.getMemoryBulkOperation(logger).setIndex(ESUtilTest.NAME_INDEX_RIVER).setType("car");
        RiverSettings settings = createSettings();
        Map<String,Object> index = (Map<String,Object>)settings.settings().get("index");
        index.put("settings","{\"analysis\":{\"filter\":{\"snowball\":{\"type\":\"snowball\",\"language\":\"French\"},\"stemmer_french\":{\"type\":\"stemmer\",\"name\":\"french\"}},\"tokenizer\":{\"ngram4\":{\"type\":\"nGram\",\"min_gram\":5,\"max_gram\":8}},\"analyzer\":{\"ngrametastem\":{\"type\":\"custom\",\"tokenizer\":\"ngram4\",\"filter\":[\"standard\",\"lowercase\",\"stemmer_french\"]} }}}");
        index.put("mapping","{\"" + ESUtilTest.TYPE_INDEX_RIVER + "\":{\"properties\":{\"title\":{\"type\":\"string\"}}}}");

        JDBCRiver river = new JDBCRiver(new RiverName("type_test","type_test"),settings,ESUtilTest.TYPE_INDEX_RIVER,op.getClient());
        river.start();

        op.getClient().admin().cluster().state(Requests.clusterStateRequest().filteredIndices(ESUtilTest.NAME_INDEX_RIVER)).actionGet().state().metaData();

    }

    /**
     * Test the set of parameters of the river (mapping...)
     */
    @Test
    public void testSettings(){
        BulkOperation op = ESUtilTest.getMemoryBulkOperation(logger).setIndex(RIVER_INDEX_NAME).setType("car");
        Map<String, Object> map = new HashMap<String, Object>();
        Map<String, Object> jdbc = new HashMap<String, Object>();
        jdbc.put("mapping",ESUtilTest.createMapping("_id","_operation","id","label","option"));
        map.put("jdbc",jdbc);
        jdbc.put("user","SA");
        RiverSettings settings = new RiverSettings(ImmutableSettings.settingsBuilder().build(),map);
        JDBCRiver river = new JDBCRiver(new RiverName("type_test","type_test"),settings,ESUtilTest.TYPE_INDEX_RIVER,op.getClient());
        Assert.assertNotNull(river);
        Assert.assertNotNull(river.mapping);
        Assert.assertEquals(5,river.mapping.size());
    }

    private RiverSettings createSettings(){


        Map<String, Object> map = new HashMap<String, Object>();
        Map<String, Object> jdbc = new HashMap<String, Object>();
        Map<String, Object> index = new HashMap<String, Object>();
        map.put("jdbc",jdbc);
        map.put("index",index);
        jdbc.put("user","SA");
        jdbc.put("password","");
        jdbc.put("driver","org.hsqldb.jdbcDriver");
        jdbc.put("url","jdbc:hsqldb:mem:test");
        jdbc.put("sql",complexQuery2);
        jdbc.put("mapping",mappingComplexQuery2);
        jdbc.put("aliasDateField","_modification_date");
        jdbc.put("fetchsize",4);
        jdbc.put("strategy","timebasis");
        index.put("index","shop");
        index.put("type","car");

        return new RiverSettings(ImmutableSettings.settingsBuilder().build(),map);
    }

    @Test
    public void testMandatoryField(){
        BulkOperation op = ESUtilTest.getMemoryBulkOperation(logger).setIndex(RIVER_INDEX_NAME).setType("car");

        RiverSettings settings = createSettings();
        ((Map<String,Object>)settings.settings().get("jdbc")).put("sql",mandatoryQuery);
        JDBCRiver river = new JDBCRiver(new RiverName(ESUtilTest.TYPE_INDEX_RIVER,ESUtilTest.TYPE_INDEX_RIVER),settings,ESUtilTest.NAME_INDEX_RIVER,op.getClient());
        river.riverStrategy.run();

    }


    @Test
    public void testComplexMergerInMemory()throws Exception{
        BulkOperation op = ESUtilTest.getMemoryBulkOperation(logger).setIndex(RIVER_INDEX_NAME).setType("car");
        ESUtilTest.createIndexIfNotExist(op.getClient(),ESUtilTest.NAME_INDEX_RIVER);

        RiverSettings settings = createSettings();
        JDBCRiver river = new JDBCRiver(new RiverName(ESUtilTest.TYPE_INDEX_RIVER,ESUtilTest.TYPE_INDEX_RIVER),settings,ESUtilTest.NAME_INDEX_RIVER,op.getClient());
        river.delay = false;
        river.riverStrategy.run();
        ESUtilTest.refreshIndex(op.getClient(), RIVER_INDEX_NAME);
        Assert.assertEquals(op.getClient().prepareSearch(RIVER_INDEX_NAME).execute().actionGet().getHits().getTotalHits(), 5);

        // Add new entry to test if it's indexed
        Connection conn = SQLUtilTest.createConnection();
        SQLUtilTest.addDataTest(conn, 6, "car6", "2012-07-02 14:00:00", new Integer[]{3});
        conn.close();


        river.riverStrategy.run();
        ESUtilTest.refreshIndex(op.getClient(), RIVER_INDEX_NAME);
        Assert.assertEquals(op.getClient().prepareSearch(RIVER_INDEX_NAME).execute().actionGet().getHits().getTotalHits(), 6);

        // Add a new object the same last modification date. We check it's really index
        conn = SQLUtilTest.createConnection();
        SQLUtilTest.addDataTest(conn, 7, "car7", "2012-07-02 14:00:00", new Integer[]{1});
        conn.close();

        river.riverStrategy.run();
        ESUtilTest.refreshIndex(op.getClient(), RIVER_INDEX_NAME);
        Assert.assertEquals(op.getClient().prepareSearch(RIVER_INDEX_NAME).execute().actionGet().getHits().getTotalHits(), 7);

        // Add a new object the same last modification date. It must be avoid
        conn = SQLUtilTest.createConnection();
        SQLUtilTest.addDataTest(conn, 8, "car8", "2012-07-02 14:00:00", new Integer[]{2,4});
        conn.close();

        river.riverStrategy.run();
        ESUtilTest.refreshIndex(op.getClient(), RIVER_INDEX_NAME);
        Assert.assertEquals(op.getClient().prepareSearch(RIVER_INDEX_NAME).execute().actionGet().getHits().getTotalHits(), 7);

    }


    @Test
    public void testDeleteDocuments()throws Exception{
        // Reset data and index
        Connection conn = SQLUtilTest.createConnection();
        SQLUtilTest.truncateTable(conn,"car");
        SQLUtilTest.truncateTable(conn,"car_opt_have");
        SQLUtilTest.truncateTable(conn,"car_color_have");

        SQLUtilTest.addDataTest(conn,1,"car1","2012-07-30 12:00:00",new Integer[]{3});
        SQLUtilTest.addDataTest(conn,2,"car2","2012-07-30 15:00:00",new Integer[]{3});

        conn.close();


        BulkOperation op = ESUtilTest.getMemoryBulkOperation(logger).setIndex(RIVER_INDEX_NAME).setType("car");
        ESUtilTest.createIndexIfNotExist(op.getClient(),ESUtilTest.NAME_INDEX_RIVER);

        RiverSettings settings = createSettings();
        ((Map<String,Object>)settings.settings().get("jdbc")).put("sql",deleteQuery);
        ((Map<String,Object>)settings.settings().get("jdbc")).put("mapping",mappingDeleteQuery);

        JDBCRiver river = new JDBCRiver(new RiverName(ESUtilTest.TYPE_INDEX_RIVER,ESUtilTest.TYPE_INDEX_RIVER),settings,ESUtilTest.NAME_INDEX_RIVER,op.getClient());
        river.delay = false;
        river.riverStrategy.run();
        ESUtilTest.refreshIndex(op.getClient(), RIVER_INDEX_NAME);
        Assert.assertEquals(op.getClient().prepareSearch(RIVER_INDEX_NAME).execute().actionGet().getHits().getTotalHits(), 2);

        conn = SQLUtilTest.createConnection();
        conn.createStatement().execute("update car set label = 'delete_car2' where id = 2");
        conn.close();

        river.delay = false;
        river.riverStrategy.run();
        ESUtilTest.refreshIndex(op.getClient(), RIVER_INDEX_NAME);
        Assert.assertEquals(op.getClient().prepareSearch(RIVER_INDEX_NAME).execute().actionGet().getHits().getTotalHits(), 1);
    }

    /**
     * Test the elimination of duplicate in list
     */
    @Test
    public void testDuplicateJoin()throws Exception{
        Connection connection = SQLUtilTest.createConnection();
        SQLUtilTest.truncateTable(connection,"car");
        SQLUtilTest.truncateTable(connection,"car_opt_have");
        SQLUtilTest.truncateTable(connection,"car_color_have");

        // Add a car with two colors and only one option
        SQLUtilTest.addDataTest(connection, 1,"car1","2012-07-12 12:00:00",new Integer[]{1},new Integer[]{2,3});
        connection.close();

        BulkOperation op = ESUtilTest.getMemoryBulkOperation(logger).setIndex(RIVER_INDEX_NAME).setType("car");
        ESUtilTest.createIndexIfNotExist(op.getClient(), ESUtilTest.NAME_INDEX_RIVER);

        RiverSettings settings = createSettings();
        ((Map<String,Object>)settings.settings().get("jdbc")).put("sql",manyJoinQuery);
        ((Map<String,Object>)settings.settings().get("jdbc")).put("mapping",mappingManyJoinQuery);

        JDBCRiver river = new JDBCRiver(new RiverName(ESUtilTest.TYPE_INDEX_RIVER,ESUtilTest.TYPE_INDEX_RIVER),settings,ESUtilTest.NAME_INDEX_RIVER,op.getClient());
        river.delay = false;
        river.riverStrategy.run();

        ESUtilTest.refreshIndex(op.getClient(), RIVER_INDEX_NAME);
        SearchResponse response = op.getClient().prepareSearch(RIVER_INDEX_NAME).execute().actionGet();
        Assert.assertEquals(response.getHits().getTotalHits(), 1);
        Assert.assertEquals(((List<Object>)response.getHits().hits()[0].sourceAsMap().get("options")).size(),1);
        Assert.assertEquals(((List<Object>)response.getHits().hits()[0].sourceAsMap().get("colors")).size(),2);
    }

    @Test
    public void testStatutExecution()throws Exception{

        // Test with bad request sql
        BulkOperation op = ESUtilTest.getMemoryBulkOperation(logger).setIndex(RIVER_INDEX_NAME).setType("car");
        ESUtilTest.createIndexIfNotExist(op.getClient(), ESUtilTest.NAME_INDEX_RIVER);

        RiverSettings settings = createSettings();
        ((Map<String,Object>)settings.settings().get("jdbc")).put("sql","select nothing from error");
        ((Map<String,Object>)settings.settings().get("jdbc")).put("mapping",ESUtilTest.createMapping("nothing"));

        JDBCRiver river = new JDBCRiver(new RiverName(ESUtilTest.TYPE_INDEX_RIVER,ESUtilTest.TYPE_INDEX_RIVER),settings,ESUtilTest.NAME_INDEX_RIVER,op.getClient());
        river.delay = false;
        river.riverStrategy.run();

        GetResponse response = op.getClient().prepareGet(ESUtilTest.NAME_INDEX_RIVER,ESUtilTest.TYPE_INDEX_RIVER,JDBCRiver.ID_INFO_RIVER_INDEX).execute().actionGet();
        Assert.assertEquals(((Map<String,Object>)response.sourceAsMap().get("jdbc")).get("statut"),"KO");
    }

    @Test
    public void testManyEntriesInMemory()throws Exception{
        // Truncate tables
        Connection connection = SQLUtilTest.createConnection();
        SQLUtilTest.truncateTable(connection,"car");
        SQLUtilTest.truncateTable(connection,"car_opt_have");
        SQLUtilTest.truncateTable(connection,"car_color_have");

        SQLUtilTest.createRandomData(connection,0,1000,2012,06,12);
        connection.close();

        BulkOperation op = ESUtilTest.getMemoryBulkOperation(logger).setIndex(RIVER_INDEX_NAME).setType("car");
        ESUtilTest.createIndexIfNotExist(op.getClient(), ESUtilTest.NAME_INDEX_RIVER);

        RiverSettings settings = createSettings();
        JDBCRiver river = new JDBCRiver(new RiverName(ESUtilTest.TYPE_INDEX_RIVER,ESUtilTest.TYPE_INDEX_RIVER),settings,ESUtilTest.NAME_INDEX_RIVER,op.getClient());
        river.delay = false;
        river.riverStrategy.run();
        ESUtilTest.refreshIndex(op.getClient(), RIVER_INDEX_NAME);
        Assert.assertEquals(op.getClient().prepareSearch(RIVER_INDEX_NAME).execute().actionGet().getHits().getTotalHits(), 1000);


        // Create new Data
        SQLUtilTest.createRandomData(null,1000,253,2012,07,12);
        river.riverStrategy.run();
        ESUtilTest.refreshIndex(op.getClient(), RIVER_INDEX_NAME);
        Assert.assertEquals(op.getClient().prepareSearch(RIVER_INDEX_NAME).execute().actionGet().getHits().getTotalHits(), 1253);
    }

}
