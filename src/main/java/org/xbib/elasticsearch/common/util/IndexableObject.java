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
package org.xbib.elasticsearch.common.util;

import java.io.IOException;
import java.util.Map;

/**
 * A structured object is composed by an object data source together with
 * meta data about the object.
 */
public interface IndexableObject {

    /**
     * Set the operation type, either "index", "create", or "delete"
     *
     * @param optype the operaion type
     * @return this indexable object
     */
    IndexableObject optype(String optype);

    /**
     * Get the operation type
     *
     * @return the operation type
     */
    String optype();

    /**
     * Set the index
     *
     * @param index the index
     * @return this object
     */
    IndexableObject index(String index);

    /**
     * Get the index
     *
     * @return the index
     */
    String index();

    /**
     * Set the type
     *
     * @param type the type
     * @return this object
     */
    IndexableObject type(String type);

    /**
     * Get the type
     *
     * @return the type
     */
    String type();

    /**
     * Set the ID
     *
     * @param id the ID
     * @return this object
     */
    IndexableObject id(String id);

    /**
     * Get the ID
     *
     * @return the ID
     */
    String id();

    /**
     * Set meta data of this indexable object
     *
     * @param key   the meta data key
     * @param value the meta data value
     * @return this object
     */
    IndexableObject meta(String key, String value);

    String meta(String key);

    IndexableObject source(Map<String, Object> source);

    Map<String, Object> source();

    String build() throws IOException;

    boolean isEmpty();

}
