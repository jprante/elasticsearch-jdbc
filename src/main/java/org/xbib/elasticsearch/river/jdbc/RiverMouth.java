package org.xbib.elasticsearch.river.jdbc;

import org.xbib.elasticsearch.plugin.jdbc.IndexableObject;
import org.xbib.elasticsearch.plugin.jdbc.RiverContext;
import org.xbib.elasticsearch.support.client.Ingest;

import java.io.IOException;

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
    RiverMouth setRiverContext(RiverContext context);

    /**
     * Set the Elasticsearch ingester
     *
     * @param ingester the ingester
     * @return this river mouth
     */
    RiverMouth setIngest(Ingest ingester);

    RiverMouth setIndex(String index);

    RiverMouth setType(String type);

    RiverMouth setId(String id);

    String getId();

    /**
     * Index. Indicating that an object has been built and is ready to be indexed.
     * The source of the document is held by the XContentBuilder.
     *
     * @param object the indexable object
     * @param create true if the document should be created
     * @throws IOException when indexing fails
     */
    void index(IndexableObject object, boolean create) throws IOException;

    /**
     * Delete. Indicating that an object should be deleted from the index.
     *
     * @param object the structured object
     * @throws IOException when delete fails
     */
    void delete(IndexableObject object) throws IOException;

    /**
     * Flush data to the river mouth
     *
     * @throws IOException when flush fails
     */
    void flush() throws IOException;

    /**
     * Close this river mouth
     */
    void close() throws IOException;

}
