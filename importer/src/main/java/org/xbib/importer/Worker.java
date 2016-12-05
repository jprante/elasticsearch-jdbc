package org.xbib.importer;

import java.io.Closeable;
import java.io.IOException;

/**
 * Worker interface.
 *
 * @param <R> request type
 */
public interface Worker<R> extends Closeable {
    /**
     * Execute a request.
     *
     * @param request request
     * @throws IOException if execution fails
     */
    void execute(R request) throws IOException;

    R getRequest();
}
