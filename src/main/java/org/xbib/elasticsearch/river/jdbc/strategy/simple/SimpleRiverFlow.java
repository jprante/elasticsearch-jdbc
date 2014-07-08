package org.xbib.elasticsearch.river.jdbc.strategy.simple;

import org.xbib.elasticsearch.plugin.feeder.jdbc.JDBCFeeder;
import org.xbib.elasticsearch.plugin.jdbc.RiverContext;
import org.xbib.elasticsearch.river.jdbc.RiverFlow;

public class SimpleRiverFlow implements RiverFlow {

    protected RiverContext context;

    protected JDBCFeeder feeder;

    @Override
    public String strategy() {
        return "simple";
    }

    @Override
    public RiverFlow setRiverContext(RiverContext context) {
        this.context = context;
        return this;
    }

    @Override
    public RiverFlow setFeeder(JDBCFeeder feeder) {
        this.feeder = feeder;
        return this;
    }

    @Override
    public JDBCFeeder getFeeder() {
        if (feeder == null) {
            this.feeder = new JDBCFeeder();
        }
        return feeder;
    }
}
