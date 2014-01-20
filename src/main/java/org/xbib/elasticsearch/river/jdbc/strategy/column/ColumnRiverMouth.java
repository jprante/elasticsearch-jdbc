
package org.xbib.elasticsearch.river.jdbc.strategy.column;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;

import org.xbib.elasticsearch.river.jdbc.strategy.simple.SimpleRiverMouth;

/**
 * River mouth implementation for the 'column' strategy
 *
 * @author Piotr Śliwa <piotr.sliwa@zineinc.com>
 */
public class ColumnRiverMouth extends SimpleRiverMouth {

    private final ESLogger logger = ESLoggerFactory.getLogger(ColumnRiverMouth.class.getName());

    protected ESLogger logger() {
        return logger;
    }

    @Override
    public String strategy() {
        return "column";
    }

}
