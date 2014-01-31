
package org.xbib.elasticsearch.river.jdbc;

import org.xbib.elasticsearch.river.jdbc.support.RiverContext;

/**
 * RiverFlow API.
 * The RiverFlow is the abstraction to the thread which
 * performs data fetching from the river source and transports it
 * to the river mouth
 *
 */
public interface RiverFlow extends Runnable {

    /**
     * The doc ID of the info document in the river index
     */
    String ID_INFO_RIVER_INDEX = "_custom";

    /**
     * The strategy name
     *
     * @return the strategy of this river task
     */
    String strategy();

    /**
     * Set river context
     *
     * @param context the context
     * @return this river flow
     */
    RiverFlow riverContext(RiverContext context);

    /**
     * Get river context
     *
     * @return the river context
     */
    RiverContext riverContext();

    /**
     * Get river state
     * @return the river state
     */
    RiverState riverState();

    /**
     * Schedule thread
     * @param thread the thread
     */
    void schedule(Thread thread);

    /**
     * Run thread once
     * @param thread the thread
     */
    void once(Thread thread);

    /**
     * Run river once
     */
    void move();

    /**
     * Abort river task. Set signal to interrupt thread and free resources.
     */
    void abort();

    boolean isActive();

}

