

package org.xbib.elasticsearch.river.jdbc.support;

import org.xbib.elasticsearch.river.jdbc.RiverFlow;
import org.xbib.elasticsearch.river.jdbc.RiverMouth;
import org.xbib.elasticsearch.river.jdbc.RiverSource;
import org.xbib.elasticsearch.river.jdbc.strategy.simple.SimpleRiverFlow;
import org.xbib.elasticsearch.river.jdbc.strategy.simple.SimpleRiverMouth;
import org.xbib.elasticsearch.river.jdbc.strategy.simple.SimpleRiverSource;

import java.util.Iterator;
import java.util.ServiceLoader;

/**
 * The river service loader looks up sources
 *
 * @author Jörg Prante <joergprante@gmail.com>
 */
public class RiverServiceLoader {

    /**
     * A river source is the origin, the data producing side
     *
     * @param strategy the strategy
     * @return a river source, or the SimpleRiverSource
     */
    public static RiverSource findRiverSource(String strategy) {
        ServiceLoader<RiverSource> sourceLoader = ServiceLoader.load(RiverSource.class);
        Iterator<RiverSource> riverSourceIterator = sourceLoader.iterator();
        while (riverSourceIterator.hasNext()) {
            RiverSource rs = riverSourceIterator.next();
            if (strategy.equals(rs.strategy())) {
                return rs;
            }
        }
        return new SimpleRiverSource();
    }

    /**
     * A river mouth is the Elasticsearch side of the river, where the bulk processor lives
     *
     * @param strategy the strategy
     * @return a river mouth, or the SimpleRiverMouth
     */
    public static RiverMouth findRiverMouth(String strategy) {
        ServiceLoader<RiverMouth> riverMouthLoader = ServiceLoader.load(RiverMouth.class);
        Iterator<RiverMouth> riverMouthIterator = riverMouthLoader.iterator();
        while (riverMouthIterator.hasNext()) {
            RiverMouth rt = riverMouthIterator.next();
            if (strategy.equals(rt.strategy())) {
                return rt;
            }
        }
        return new SimpleRiverMouth();
    }

    /**
     * A river flow encapsulates the thread that moves the data from source to mouth
     *
     * @param strategy the strategy
     * @return a river flow, or the SimpleRiverFlow
     */
    public static RiverFlow findRiverFlow(String strategy) {
        ServiceLoader<RiverFlow> riverFlowServiceLoader = ServiceLoader.load(RiverFlow.class);
        Iterator<RiverFlow> riverFlowIterator = riverFlowServiceLoader.iterator();
        while (riverFlowIterator.hasNext()) {
            RiverFlow rc = riverFlowIterator.next();
            if (strategy.equals(rc.strategy())) {
                return rc;
            }
        }
        return new SimpleRiverFlow();
    }

}
