
package org.xbib.elasticsearch.river.jdbc;

import java.io.IOException;
import java.util.Map;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.xbib.elasticsearch.river.jdbc.support.RiverContext;
import org.xbib.elasticsearch.gatherer.IndexableObject;

/**
 * The river mouth is the abstraction of the destination where all the data
 * is flowing from the river source.
 */
public interface RiverMouth {

    /**
     * The river strategy
     *
     * @return the strategy
     */
    String strategy();

    /**
     * The river context
     *
     * @param context the river context
     * @return this river mouth
     */
    RiverMouth riverContext(RiverContext context);

    /**
     * Set the Elasticsearch client
     *
     * @param client the client
     * @return this river mouth
     */
    RiverMouth client(Client client);

    /**
     * Return the Elasticsearch client
     *
     * @return this river mouth
     */
    Client client();

    /**
     * Set settings for Elasticsearch to be used in index creation
     * @param settings the settings
     * @return this river mouth
     */
    RiverMouth setSettings(Map<String,Object> settings);

    /**
     * Set mapping for Elasticsearch to be used in mapping creation
     * @param mapping the mapping
     * @return this river mouth
     */
    RiverMouth setMapping(Map<String,Object> mapping);

    /**
     * Set index
     *
     * @param index the index
     * @return this river mouth
     */
    RiverMouth setIndex(String index);

    /**
     * Return index
     *
     * @return the index
     */
    String getIndex();

    /**
     * Set type
     *
     * @param type the type
     * @return this river mouth
     */
    RiverMouth setType(String type);

    /**
     * Return type
     *
     * @return the type
     */
    String getType();

    /**
     * Set ID
     *
     * @param id the id
     * @return this river mouth
     */
    RiverMouth setId(String id);

    String getId();

    /**
     * Set maximum number of bulk actions
     *
     * @param actions the number of bulk actions
     * @return this river mouth
     */
    RiverMouth setMaxBulkActions(int actions);

    /**
     * Set maximum number of concurrent bulk requests
     *
     * @param max the  maximum number of concurrent bulk requests
     * @return this river mouth
     */
    RiverMouth setMaxConcurrentBulkRequests(int max);

    /**
     * Set maximum volume of a bulk request
     * @param maxVolumePerBulkRequest the maxiumum byte size of a buk request
     * @return this river mouth
     */
    RiverMouth setMaxVolumePerBulkRequest(ByteSizeValue maxVolumePerBulkRequest);

    /**
     * Set flush interval
     * @param flushInterval flush interval
     * @return this river mouth
     */
    RiverMouth setFlushInterval(TimeValue flushInterval);

    /**
     * Index. Indicating that an object has been built and is ready to be indexed.
     * The source of the document is held by the XContentBuilder.
     *
     * @param object the indexable object
     * @param create true if the document should be created
     * @throws IOException
     */
    void index(IndexableObject object, boolean create) throws IOException;

    /**
     * Delete. Indicating that an object should be deleted from the index.
     *
     * @param object the structured object
     * @throws IOException
     */
    void delete(IndexableObject object) throws IOException;

    /**
     * Flush data to the river mouth
     *
     * @throws IOException
     */
    void flush() throws IOException;

    /**
     * Close this river mouth
     */
    void close();

    void waitForCluster() throws IOException;

}
