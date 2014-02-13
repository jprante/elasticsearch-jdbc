
package org.xbib.elasticsearch.gatherer;

import java.util.EnumSet;
import java.util.Set;

import static org.elasticsearch.common.collect.Sets.newHashSet;

/**
 * The names of keys with a special meaning for controlling Elasticsearch indexing.
 *
 * Mostly, they map to the Elasticsearch bulk item control keys.
 *
 *
 * The _job column denotes an ID for the event of a fetch execution.
 */
public enum ControlKeys {

    _optype, _index, _type, _id, _version, _timestamp, _ttl, _routing, _parent, _source, _job;

    public static Set<String> makeSet() {
        Set<String> set = newHashSet();
        for (ControlKeys k : EnumSet.allOf(ControlKeys.class)) {
            set.add(k.name());
        }
        return set;
    }

}