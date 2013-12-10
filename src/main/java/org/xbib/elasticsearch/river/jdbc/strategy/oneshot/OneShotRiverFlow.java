
package org.xbib.elasticsearch.river.jdbc.strategy.oneshot;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.xbib.elasticsearch.river.jdbc.strategy.simple.SimpleRiverFlow;

/**
 * One-shot river flow
 *
 * @author Jörg Prante <joergprante@gmail.com>
 */
public class OneShotRiverFlow extends SimpleRiverFlow {

    private final ESLogger logger = ESLoggerFactory.getLogger(OneShotRiverFlow.class.getSimpleName());

    protected ESLogger logger() {
        return logger;
    }

    @Override
    public String strategy() {
        return "oneshot";
    }

    @Override
    public void run() {
        move();
    }
}
