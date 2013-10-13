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
package org.xbib.elasticsearch.river.jdbc.support;

import java.io.IOException;
import java.util.Map;

/**
 * A structured object is composed by an object data source together with
 * meta data about the object.
 *
 * @author Jörg Prante <joergprante@gmail.com>
 */
public interface StructuredObject extends PseudoColumnNames, Comparable<StructuredObject> {

    StructuredObject optype(String optype);

    String optype();

    StructuredObject index(String index);

    String index();

    StructuredObject type(String type);

    String type();

    StructuredObject id(String id);

    String id();

    StructuredObject meta(String key, String value);

    String meta(String key);

    StructuredObject source(Map<String, ? super Object> source);

    Map source();

    String build() throws IOException;

    boolean isEmpty();

}
