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
package org.elasticsearch.river.jdbc;

import java.io.IOException;
import org.elasticsearch.common.xcontent.XContentBuilder;

public interface Action {

    /**
     * Create action.  Indicating that an object has been built and is ready to be indexed.
     * The source of the document is held by the XContentBuilder.
     * The document is only indexed if no document with that index/type/id exists.
     * 
     * @param index the index
     * @param type the type
     * @param id the id
     * @param version the version
     * @param builder the builder
     * @throws IOException 
     */
    void create(String index, String type, String id, long version, XContentBuilder builder) 
            throws IOException;

    /**
     * Index action. Indicating that an object has been built and is ready to be indexed.
     * The source of the document is held by the XContentBuilder.
     * @param index the index
     * @param type the type 
     * @param id the id 
     * @param version the version
     * @param builder the XContentBuilder
     * @throws IOException 
     */
    void index(String index, String type, String id, long version, XContentBuilder builder) 
            throws IOException;

    /**
     * Delete action. Indicating that an object should be deleted from the index.
     * @param index
     * @param type
     * @param id
     * @throws IOException 
     */
    void delete(String index, String type, String id)
            throws IOException;

}
