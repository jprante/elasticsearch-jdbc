/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.xbib.elasticsearch.river.jdbc;

import org.elasticsearch.client.Client;
import org.xbib.elasticsearch.river.jdbc.support.RiverContext;
import org.xbib.elasticsearch.river.jdbc.support.StructuredObject;
import org.xbib.elasticsearch.river.jdbc.support.StructuredObject;

import java.io.IOException;

/**
 * The river mouth is the abstraction of the destination where all the data
 * is flowing from the river source.
 *
 * @author Jörg Prante <joergprante@gmail.com>
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
     * Set index
     *
     * @param index the index
     * @return this river mouth
     */
    RiverMouth index(String index);

    /**
     * Return index
     * @return the index
     */
    String index();

    /**
     * Set type
     * @param type the type
     * @return this river mouth
     */
    RiverMouth type(String type);

    /**
     * Return type
     * @return the type
     */
    String type();

    /**
     * Set ID
     * @param id the id
     * @return this river mouth
     */
    RiverMouth id(String id);

    /**
     * Set maximum number of bulk actions
     *
     * @param actions the number of bulk actions
     * @return this river mouth
     */
    RiverMouth maxBulkActions(int actions);

    /**
     * Get maximum number of bulk actions
     * @return max bulk actions
     */
    int maxBulkActions();

    /**
     * Set maximum number of concurrent bulk requests
     * @param max the  maximum number of concurrent bulk requests
     * @return this river mouth
     */
    RiverMouth maxConcurrentBulkRequests(int max);

    /**
     * Get maximum concurrent bulk requests
     * @return maximum concurrent requests
     */
    int maxConcurrentBulkRequests();

    /**
     * Set versioning
     * @param enable true if versioning
     * @return this river mouth
     */
    RiverMouth versioning(boolean enable);

    /**
     * Get versioning
     * @return the versioning
     */
    boolean versioning();

    /**
     * Set acknowledge
     * @param enable true for enable
     * @return this river mouth
     */
    RiverMouth acknowledge(boolean enable);

    /**
     * Get acknowledge
     * @return true if the acknowledge is enabled
     */
    boolean acknowledge();

    /**
     * Create.  Indicating that an object has been built and is ready to be indexed.
     * The source of the document is held by the XContentBuilder.
     * The document is only indexed if no document with that index/type/id exists.
     *
     * @param object the structured object
     * @throws IOException
     */
    void create(StructuredObject object) throws IOException;

    /**
     * Index. Indicating that an object has been built and is ready to be indexed.
     * The source of the document is held by the XContentBuilder.
     *
     * @param object the indexable object
     * @throws IOException
     */
    void index(StructuredObject object) throws IOException;

    /**
     * Delete. Indicating that an object should be deleted from the index.
     *
     * @param object the structured object
     * @throws IOException
     */
    void delete(StructuredObject object) throws IOException;

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

    /**
     * Create index with optional settings and mapping
     * @param settings the settings
     * @param mapping the mapping
     */
    void createIndexIfNotExists(String settings, String mapping);

}
