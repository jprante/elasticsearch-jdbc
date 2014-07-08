package org.xbib.elasticsearch.river.jdbc;

import org.xbib.elasticsearch.plugin.feeder.jdbc.JDBCFeeder;
import org.xbib.elasticsearch.plugin.jdbc.RiverContext;

public interface RiverFlow {
    /**
     * The river strategy
     *
     * @return the strategy
     */
    String strategy();

    RiverFlow setRiverContext(RiverContext riverContext);

    /**
     * Set the feeder
     *
     * @param feeder the feeder
     * @return this river flow
     */
    RiverFlow setFeeder(JDBCFeeder feeder);

    /**
     * Return the feeder
     *
     * @return the feeder
     */
    JDBCFeeder getFeeder();
}
