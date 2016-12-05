package org.xbib.importer;

import java.io.Closeable;
import java.io.IOException;

/**
 * A Source.
 *
 */
public interface Source extends Closeable {

    /**
     * Executes action on the database.
     * @param importerListener importer listener
     * @throws IOException when execution gives an error
     */
    void execute(ImporterListener importerListener) throws IOException;
}
