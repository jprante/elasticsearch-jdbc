/*
 * Copyright (C) 2015 Jörg Prante
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.xbib.elasticsearch.jdbc.strategy;

import org.elasticsearch.common.settings.Settings;
import org.xbib.elasticsearch.common.util.IndexableObject;
import org.xbib.elasticsearch.support.client.Metric;

import java.io.IOException;
import java.util.Map;

/**
 * The sink is the abstraction of a destination where all the data
 * is transported to.
 */
public interface Sink<C extends Context> {

    /**
     * The strategy
     *
     * @return the strategy
     */
    String strategy();

    /**
     * Create a new sink instance
     *
     * @return new sink instance
     */
    Sink<C> newInstance();

    /**
     * Set the context
     *
     * @param context the context
     * @return this sink
     */
    Sink<C> setContext(C context);

    /**
     * Set index settings
     *
     * @param indexSettings the index settings
     * @return this sink
     */
    Sink setIndexSettings(Settings indexSettings);

    /**
     * Set index type mappings
     *
     * @param typeMapping the index type mappings
     * @return this sink
     */
    Sink setTypeMapping(Map<String, String> typeMapping);

    /**
     * Executed before source fetch
     *
     * @throws Exception if this method fails
     */
    void beforeFetch() throws Exception;

    /**
     * Executed after source fetch
     *
     * @throws Exception if this method fails
     */
    void afterFetch() throws Exception;

    /**
     * Set default index name for this sink
     *
     * @param index the default index name
     * @return this sink
     */
    Sink setIndex(String index);

    /**
     * Get default index name of this sink
     *
     * @return the index name
     */
    String getIndex();

    /**
     * Set default index type name for this sink
     *
     * @param type the default index type name for this sink
     * @return this sink
     */
    Sink setType(String type);

    /**
     * Get default index type name of this sink
     *
     * @return the default index type name of this sink
     */
    String getType();

    /**
     * Set document identifier for next document to be indexed
     *
     * @param id the id
     * @return this sink
     */
    Sink setId(String id);

    /**
     * Get document identifier
     *
     * @return the document identifier
     */
    String getId();

    /**
     * Index operation. Indicating that an object has been built and is ready to be indexed.
     * The source of the document is held by the XContentBuilder.
     *
     * @param object the indexable object
     * @param create true if the document should be created
     * @throws IOException when indexing fails
     */
    void index(IndexableObject object, boolean create) throws IOException;

    /**
     * Delete operation. Indicating that an object should be deleted from the index.
     *
     * @param object the structured object
     * @throws IOException when delete fails
     */
    void delete(IndexableObject object) throws IOException;

    /**
     * Shutdown and release all resources, e.g. bulk processor and client
     * @throws IOException when shutdown fails
     */
    void shutdown() throws IOException;

    Sink setMetric(Metric metric);

    Metric getMetric();
}
