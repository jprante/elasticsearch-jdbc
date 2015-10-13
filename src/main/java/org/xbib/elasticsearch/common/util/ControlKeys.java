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

import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

/**
 * The names of keys with a special meaning for controlling Elasticsearch indexing.
 * Mostly, they map to the Elasticsearch bulk item control keys.
 * The _job column denotes an ID for the event of a fetch execution.
 */
public enum ControlKeys {

    _optype, _index, _type, _id, _version, _timestamp, _ttl, _routing, _parent, _source, _job;

    public static Set<String> makeSet() {
        Set<String> set = new HashSet<>();
        for (ControlKeys k : EnumSet.allOf(ControlKeys.class)) {
            set.add(k.name());
        }
        return set;
    }

}
