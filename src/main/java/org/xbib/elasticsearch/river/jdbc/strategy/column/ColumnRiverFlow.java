package org.xbib.elasticsearch.river.jdbc.strategy.column;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.xbib.elasticsearch.plugin.feeder.jdbc.JDBCFeeder;
import org.xbib.elasticsearch.river.jdbc.RiverFlow;
import org.xbib.elasticsearch.river.jdbc.strategy.simple.SimpleRiverFlow;

/**
 * River flow implementation for the 'column' strategy
 *
 * @author <a href="piotr.sliwa@zineinc.com">Piotr Śliwa</a>
 */
public class ColumnRiverFlow extends SimpleRiverFlow {

    private final ESLogger logger = ESLoggerFactory.getLogger(ColumnRiverFlow.class.getName());

    public static final String DOCUMENT = "_custom";

    public static final String LAST_RUN_TIME = "last_run_time";

    public static final String CURRENT_RUN_STARTED_TIME = "current_run_started_time";

    protected ESLogger logger() {
        return logger;
    }

    @Override
    public String strategy() {
        return "column";
    }

    @Override
    public RiverFlow setFeeder(JDBCFeeder feeder) {
        this.feeder = feeder;
        return this;
    }

    @Override
    public JDBCFeeder getFeeder() {
        if (feeder == null) {
            this.feeder = new ColumnRiverFeeder();
        }
        return feeder;
    }

}
