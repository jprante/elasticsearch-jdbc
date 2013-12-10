
package org.xbib.elasticsearch.river.jdbc.strategy.oneshot;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.xbib.elasticsearch.river.jdbc.strategy.simple.SimpleRiverSource;

/**
 * One-shot river source
 *
 * @author Jörg Prante <joergprante@gmail.com>
 */
public class OneShotRiverSource extends SimpleRiverSource {

    private final ESLogger logger = ESLoggerFactory.getLogger(OneShotRiverSource.class.getSimpleName());

    protected ESLogger logger() {
        return logger;
    }

    @Override
    public String strategy() {
        return "oneshot";
    }
}
