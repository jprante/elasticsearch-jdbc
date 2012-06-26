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

public class DefaultAction implements Action {

    @Override
    public void create(String index, String type, String id, long version, XContentBuilder builder) throws IOException {
    }

    @Override
    public void index(String index, String type, String id, long version, XContentBuilder builder) throws IOException {
    }

    @Override
    public void delete(String index, String type, String id) throws IOException {
    }
    
}
