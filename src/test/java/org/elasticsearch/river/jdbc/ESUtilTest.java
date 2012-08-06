package org.elasticsearch.river.jdbc;

import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 * Give methods to manage ES during test
 */
public class ESUtilTest {

    public static void refreshIndex(Client client,String index){
        client.admin().indices().refresh(new RefreshRequest(index)).actionGet();
    }

    public static void createIndexIfNotExist(Client client,String index){
        // Creation of working index
        if(client.admin().indices().prepareExists(index).execute().actionGet().exists() == false){
            client.admin().indices().prepareCreate(index).execute().actionGet();
        }
    }

    public static void deleteDocumentsInIndex(Client client,String index){
        client.prepareDeleteByQuery(index).setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
    }

    public static void deleteDocumentInIndex(Client client,String index, String type,String id){
        client.prepareDelete(index, type, id).execute().actionGet();
    }

    public static List<Object> createMapping(String...args){
        List<Object> mapping = new ArrayList<Object>();
        for(String str : args){
            mapping.add(str);
        }
        return mapping;
    }

    /**
     * Give of logger mock of BulkOperation
     * @param logger
     * @return
     */
    public static BulkOperation getMockBulkOperation(ESLogger logger){
        return new BulkOperation(null,logger){
            @Override
            public void create(String index, String type, String id, long version, XContentBuilder builder) {
                try{
                    logger.info(" CREATE : " + this.index + "/" + this.type + "/" + id + " => " + builder.string());
                }catch(Exception e){
                    logger.error("Error generating CREATE");
                }
            }

            @Override
            public void index(String index, String type, String id, long version, XContentBuilder builder) {
                try{
                    logger.info(" INDEX: " + this.index + "/" + this.type + "/" + id + " => " + builder.string());
                }catch(Exception e){
                    logger.error("Error generating INDEX");
                }
            }

            @Override
            public void delete(String index, String type, String id) {
                logger.info(" DELETE: " + this.index + "/" + this.type + "/" + id);
            }
        };
    }

    /**
     * Create a test BulkOperation with ES in memory
     * @param logger
     * @return
     */
    public static BulkOperation getMemoryBulkOperation(ESLogger logger){
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("gateway.type","none")
                .put("index.gateway.type", "none")
                .put("index.store.type", "memory")
                .put("path.data","target/data")
                .build();

        Node node = NodeBuilder.nodeBuilder().local(true).settings(settings).node();
        final Client client = node.client();


        return new BulkOperation(client,logger){
            @Override
            public BulkOperation setIndex(String index) {
                super.setIndex(index);
                // We create the index in memory if it does'nt exist
                if(!client.admin().indices().prepareExists(index).execute().actionGet().exists()){
                    client.admin().indices().prepareCreate(index).execute().actionGet();
                }
                return this;
            }

            @Override
            public void create(String index, String type, String id, long version, XContentBuilder builder) {
                try{
                    logger.info(" CREATE : " + this.index + "/" + this.type + "/" + id + " => " + builder.string());
                    client.prepareIndex(this.index, this.type).setSource(builder).execute().actionGet();
                }catch(Exception e){
                    logger.error("Error generating CREATE");
                }
            }

            @Override
            public void index(String index, String type, String id, long version, XContentBuilder builder) {
                try{
                    logger.info(" INDEX: " + this.index + "/" + this.type + "/" + id + " => " + builder.string());
                    IndexResponse r = client.prepareIndex(this.index,this.type,id).setSource(builder).execute().actionGet();
                }catch(Exception e){
                    logger.error("Error generating INDEX");
                }
            }

            @Override
            public void delete(String index, String type, String id) {
                logger.info(" DELETE: " + this.index + "/" + this.type + "/" + id);
                client.prepareDelete(this.index, this.type, id).execute().actionGet();
            }
        };
    }

}
