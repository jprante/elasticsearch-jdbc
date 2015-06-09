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
package org.xbib.elasticsearch.common.keyvalue;

import java.io.IOException;
import java.util.List;

public interface KeyValueStreamListener<K, V> {

    /**
     * Begin a key/value sequence
     *
     * @return this listener
     * @throws java.io.IOException if this method fails
     */
    KeyValueStreamListener<K, V> begin() throws IOException;

    /**
     * Receive key/value pair
     *
     * @param key   the key
     * @param value the value
     * @return this istener
     * @throws java.io.IOException if this method fails
     */
    KeyValueStreamListener<K, V> keyValue(K key, V value) throws IOException;

    /**
     * Declare the keys for the values
     *
     * @param keys the keys
     * @return this listener
     * @throws java.io.IOException if this method fails
     */
    KeyValueStreamListener<K, V> keys(List<K> keys) throws IOException;

    /**
     * Receive values for the declared keys
     *
     * @param values the values
     * @return this listener
     * @throws java.io.IOException if this method fails
     */
    KeyValueStreamListener<K, V> values(List<V> values) throws IOException;

    /**
     * End a key/value sequence
     *
     * @return this listener
     * @throws java.io.IOException if this method fails
     */
    KeyValueStreamListener<K, V> end() throws IOException;

}
