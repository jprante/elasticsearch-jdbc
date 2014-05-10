
package org.xbib.elasticsearch.river.jdbc;

import org.xbib.elasticsearch.plugin.feeder.Feeder;
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
     * @param feeder the feeder
     * @return this river flow
     */
    RiverFlow setFeeder(Feeder feeder);

    /**
     * Return the feeder
     * @return the feeder
     */
    Feeder getFeeder();
}
