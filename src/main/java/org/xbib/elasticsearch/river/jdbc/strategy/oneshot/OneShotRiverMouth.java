
package org.xbib.elasticsearch.river.jdbc.strategy.oneshot;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.xbib.elasticsearch.river.jdbc.strategy.simple.SimpleRiverMouth;

/**
 * One-shot river mouth
 *
 * @author Jörg Prante <joergprante@gmail.com>
 */
public class OneShotRiverMouth extends SimpleRiverMouth {

    private static final ESLogger logger = ESLoggerFactory.getLogger(OneShotRiverMouth.class.getSimpleName());

    protected ESLogger logger() {
        return logger;
    }

    @Override
    public String strategy() {
        return "oneshot";
    }
}
