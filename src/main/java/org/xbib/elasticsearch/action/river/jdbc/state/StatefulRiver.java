package org.xbib.elasticsearch.action.river.jdbc.state;

import org.elasticsearch.river.River;

/**
 * A stateful river is an extension of a river with a river state
 */
public interface StatefulRiver extends River {

    RiverState getRiverState();
}
