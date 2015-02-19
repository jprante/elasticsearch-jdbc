/*
 * Copyright (C) 2014 Jörg Prante
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
import org.xbib.elasticsearch.jdbc.client.IngestFactory;
import org.xbib.elasticsearch.jdbc.client.Metric;
import org.xbib.elasticsearch.jdbc.util.IndexableObject;

import java.io.IOException;
import java.util.Map;

/**
 * The mouth is the abstraction of the destination where all the data
 * is flowing from the source.
 */
public interface Mouth<C extends Context> {

    /**
     * The strategy
     *
     * @return the strategy
     */
    String strategy();

    /**
     * Create a new mouth instance
     *
     * @return new mouth instance
     */
    Mouth<C> newInstance();

    /**
     * Set the context
     *
     * @param context the context
     * @return this mouth
     */
    Mouth<C> setContext(C context);

    /**
     * Set ingest factory
     *
     * @param ingestFactory the ingest factory
     * @return this mouth
     */
    Mouth setIngestFactory(IngestFactory ingestFactory);

    /**
     * Get the metrics of this mouth
     *
     * @return this mouth
     */
    Metric getMetric();

    /**
     * Set index settings
     *
     * @param indexSettings the index settings
     * @return this mouth
     */
    Mouth setIndexSettings(Settings indexSettings);

    /**
     * Set index type mappings
     *
     * @param typeMapping the index type mappings
     * @return this mouth
     */
    Mouth setTypeMapping(Map<String, String> typeMapping);

    /**
     * Executed before source fetch
     *
     * @throws Exception
     */
    void beforeFetch() throws Exception;

    /**
     * Executed after source fetch
     *
     * @throws Exception
     */
    void afterFetch() throws Exception;

    /**
     * Set default index name for this mouth
     *
     * @param index the default index name
     * @return this mouth
     */
    Mouth setIndex(String index);

    /**
     * Get default index name of this mouth
     *
     * @return the index name
     */
    String getIndex();

    /**
     * Set default index type name for this mouth
     *
     * @param type the default index type name for this mouth
     * @return this mouth
     */
    Mouth setType(String type);

    /**
     * Get default index type name of this mouth
     *
     * @return the default index type name of this mouth
     */
    String getType();

    /**
     * Set document identifier for next document to be indexed
     *
     * @param id the id
     * @return this mouth
     */
    Mouth setId(String id);

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
     * Flush data to the mouth
     *
     * @throws IOException when flush fails
     */
    void flush() throws IOException;

    /**
     * Shutdown mouth and release all resources, e.g. bulk processor and client
     */
    void shutdown() throws IOException;

    /**
     * Suspend mouth. Do not proceed with indexing and wait for resume.
     *
     * @throws Exception
     */
    void suspend() throws Exception;

    /**
     * Resume mouth after suspend.
     *
     * @throws Exception
     */
    void resume() throws Exception;

}
