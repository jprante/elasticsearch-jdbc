package org.xbib.elasticsearch.support.client;

import org.elasticsearch.client.Client;

/**
 * Minimal API for feed
 */
public interface Feed {

    Client client();

    /**
     * Index document
     *
     * @param index  the index
     * @param type   the type
     * @param id     the id
     * @param source the source
     * @return this
     */
    Feed index(String index, String type, String id, String source);

    /**
     * Delete document
     *
     * @param index the index
     * @param type  the type
     * @param id    the id
     * @return this
     */
    Feed delete(String index, String type, String id);

}
