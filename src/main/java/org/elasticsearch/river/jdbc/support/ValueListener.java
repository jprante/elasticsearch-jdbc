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
package org.elasticsearch.river.jdbc.support;

import java.io.IOException;
import java.security.MessageDigest;
import java.util.List;

/**
 * Value listener interface for receiving a stream of key/value attributes,
 * like a number of rows of columns.
 *
 * @author Jörg Prante <joergprante@gmail.com>
 */
public interface ValueListener {

    /**
     * Set the key names for the values
     *
     * @param keys the keys
     * @return this ValueListener
     */
    ValueListener keys(List<String> keys);

    /**
     * Receive values for the keys
     *
     * @param values the values
     * @return this ValueListener
     * @throws IOException
     */
    ValueListener values(List<? extends Object> values) throws IOException;

    /**
     * Reset the key/value configuration
     *
     * @return this value listener
     * @throws IOException
     */
    ValueListener reset() throws IOException;

    /**
     * Compute digest value for these values.
     * @return digest
     */
    MessageDigest digest();
}
