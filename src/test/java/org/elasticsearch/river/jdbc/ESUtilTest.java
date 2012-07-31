package org.elasticsearch.river.jdbc;

import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;

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
        client.prepareDelete(index,type,id).execute().actionGet();
    }
}
