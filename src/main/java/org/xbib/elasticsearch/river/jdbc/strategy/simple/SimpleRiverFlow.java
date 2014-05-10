
package org.xbib.elasticsearch.river.jdbc.strategy.simple;

import org.xbib.elasticsearch.plugin.feeder.Feeder;
import org.xbib.elasticsearch.plugin.feeder.jdbc.JDBCFeeder;
import org.xbib.elasticsearch.plugin.jdbc.RiverContext;
import org.xbib.elasticsearch.river.jdbc.RiverFlow;

public class SimpleRiverFlow  implements RiverFlow {

    protected RiverContext context;

    protected Feeder feeder;

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
    public RiverFlow setFeeder(Feeder feeder) {
        this.feeder = feeder;
        return this;
    }

    @Override
    public Feeder getFeeder() {
        if (feeder == null) {
            this.feeder = new JDBCFeeder();
        }
        return feeder;
    }
}
