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

import org.elasticsearch.common.Base64;

import java.io.IOException;
import java.security.MessageDigest;

/**
 * A structured object with a digest.
 *
 * @author Jörg Prante <joergprante@gmail.com>
 */
public class StructuredDigestObject extends StructuredObject {

    private final static String DIGEST_ENCODING = "UTF-8";
    private MessageDigest digest;

    @Override
    public StructuredObject digest(MessageDigest digest) {
        this.digest = digest;
        return this;
    }

    @Override
    public MessageDigest digest() {
        return digest;
    }

    @Override
    public void checksum(String data) throws IOException {
        if (digest != null) {
            digest.update(data.getBytes(DIGEST_ENCODING));
        }
    }

    public void reset() {
        if (digest != null) {
            digest.reset();
        }
    }

    /**
     * Return a message digest (in base64-encoded form)
     *
     * @return the message digest
     */
    @Override
    public String checksum() {
        return digest != null ? Base64.encodeBytes(digest.digest()) : null;
    }
}
