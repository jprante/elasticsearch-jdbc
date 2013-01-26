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

/**
 * The names of the column names with a special meaning for the river execution.
 * <p/>
 * Mostly, they map to the Elasticsearch bulk item. The _job column denotes
 * an ID for the event of a fetch execution.
 *
 * @author Jörg Prante <joergprante@gmail.com>
 */
public interface PseudoColumnNames {

    // bulk job name
    String JOB = "_job";

    // bulk operation type
    String OPTYPE = "_optype";

    // bulk document
    String INDEX = "_index";
    String TYPE = "_type";
    String ID = "_id";

    // bulk parameters
    String VERSION = "_version";
    String ROUTING = "_routing";
    String PERCOLATE = "_percolate";
    String PARENT = "_parent";
    String TIMESTAMP = "_timestamp";
    String TTL = "_ttl";

    // JSON
    String SOURCE = "_source";
}
