package org.xbib.importer;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;

/**
 * The sink is the abstraction of a destination where all the data
 * is transported to.
 */
public interface Sink extends Flushable, Closeable {

    /**
     * Index operation. Indicating that an object has been built and is ready to be indexed.
     * The source of the document is held by the XContentBuilder.
     *
     * @param object the object
     * @param create true if the document should be created
     * @throws IOException when indexing fails
     */
    void index(Document object, boolean create) throws IOException;

    /**
     * Delete operation. Indicating that an object should be deleted from the index.
     *
     * @param object the object
     * @throws IOException when delete fails
     */
    void delete(Document object) throws IOException;

    /**
     * Update operation. Indicating that an object should be updated.
     *
     * @param object the object
     * @throws IOException when delete fails
     */
    void update(Document object) throws IOException;

}
