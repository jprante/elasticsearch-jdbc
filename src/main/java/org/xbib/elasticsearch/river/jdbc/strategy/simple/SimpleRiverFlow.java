
package org.xbib.elasticsearch.river.jdbc.strategy.simple;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.indices.IndexMissingException;
import org.xbib.elasticsearch.river.jdbc.RiverFlow;
import org.xbib.elasticsearch.river.jdbc.RiverMouth;
import org.xbib.elasticsearch.river.jdbc.RiverSource;
import org.xbib.elasticsearch.river.jdbc.support.RiverContext;

import java.io.IOException;
import java.util.Date;
import java.util.Map;

import static org.elasticsearch.client.Requests.indexRequest;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * A river flow implementation for the 'simple' strategy.
 * <p/>
 * This river flow runs fetch actions in a loop and waits before the next cycle begins.
 * <p/>
 * A version counter is incremented each time a fetch move is executed.
 * <p/>
 * The state of the river flow is saved between runs. So, in case of a restart, the
 * river flow will recover with the last known state of the river.
 *
 * @author Jörg Prante <joergprante@gmail.com>
 */
public class SimpleRiverFlow implements RiverFlow {

    private final ESLogger logger = ESLoggerFactory.getLogger(SimpleRiverFlow.class.getSimpleName());

    protected RiverContext context;

    protected Date startDate;

    protected boolean abort = false;

    protected ESLogger logger() {
        return logger;
    }

    @Override
    public String strategy() {
        return "simple";
    }

    @Override
    public SimpleRiverFlow riverContext(RiverContext context) {
        this.context = context;
        return this;
    }

    @Override
    public RiverContext riverContext() {
        return context;
    }

    /**
     * Set a start date
     *
     * @param creationDate the creation date
     */
    @Override
    public SimpleRiverFlow startDate(Date creationDate) {
        this.startDate = creationDate;
        return this;
    }

    /**
     * Return the start date of the river task
     *
     * @return the creation date
     */
    @Override
    public Date startDate() {
        return startDate;
    }

    /**
     * Delay the connector for poll millis, and notify a reason.
     *
     * @param reason the reason for the dealy
     */
    @Override
    public SimpleRiverFlow delay(String reason) {
        TimeValue poll = context.pollingInterval();
        if (poll.millis() > 0L) {
            logger().info("{}, waiting {}", reason, poll);
            try {
                Thread.sleep(poll.millis());
            } catch (InterruptedException e) {
                logger().debug("Thread interrupted while waiting, stopping");
                abort();
            }
        }
        return this;
    }

    /**
     * Triggers flag to abort the connector down at next run.
     */
    @Override
    public void abort() {
        this.abort = true;
    }

    /**
     * The river task loop. Execute move, check if the task must be aborted, continue with next run after a delay.
     */
    @Override
    public void run() {
        while (!abort) {
            move();
            if (abort) {
                return;
            }
            delay("next run");
        }
    }

    /**
     * A single river move.
     */
    @Override
    public void move() {
        try {
            RiverSource source = context.riverSource();
            RiverMouth riverMouth = context.riverMouth();
            Client client = context.riverMouth().client();
            Number version;
            GetResponse get = null;

            // wait for cluster health
            riverMouth.waitForCluster();

            try {
                // read state from _custom
                client.admin().indices().prepareRefresh(context.riverIndexName()).execute().actionGet();
                get = client.prepareGet(context.riverIndexName(), context.riverName(), ID_INFO_RIVER_INDEX).execute().actionGet();
            } catch (IndexMissingException e) {
                logger().warn("river state missing: {}/{}/{}", context.riverIndexName(), context.riverName(), ID_INFO_RIVER_INDEX);
            }
            if (get != null && get.isExists()) {
                Map jdbcState = (Map) get.getSourceAsMap().get("jdbc");
                if (jdbcState != null) {
                    version = (Number) jdbcState.get("version");
                    version = version == null ? 1L : version.longValue() + 1; // increase to next version
                } else {
                    throw new IOException("can't retrieve previously persisted state from " + context.riverIndexName() + "/" + context.riverName());
                }
            } else {
                version = 1L;
            }

            // save state, write activity flag
            try {
                XContentBuilder builder = jsonBuilder();
                builder.startObject().startObject("jdbc");
                if (startDate != null) {
                    builder.field("created", startDate);
                }
                builder.field("since", new Date())
                        .field("active", true);
                builder.endObject().endObject();
                client.index(indexRequest(context.riverIndexName())
                        .type(riverContext().riverName())
                        .id(ID_INFO_RIVER_INDEX)
                        .source(builder)).actionGet();
            } catch (Exception e) {
                logger().error(e.getMessage(), e);
            }


            // set default job name to current version number
            context.job(Long.toString(version.longValue()));
            String mergeDigest = source.fetch();
            // this end is required before house keeping starts
            riverMouth.flush();

            // save state
            try {
                // save state to _custom
                XContentBuilder builder = jsonBuilder();
                builder.startObject().startObject("jdbc");
                if (startDate != null) {
                    builder.field("created", startDate);
                }
                builder.field("version", version.longValue())
                        .field("digest", mergeDigest)
                        .field("since", new Date())
                        .field("active", false);
                builder.endObject().endObject();
                if (logger().isDebugEnabled()) {
                    logger().debug(builder.string());
                }
                client.index(indexRequest(context.riverIndexName())
                        .type(context.riverName())
                        .id(ID_INFO_RIVER_INDEX)
                        .source(builder))
                        .actionGet();
            } catch (Exception e) {
                logger().error(e.getMessage(), e);
            }

        } catch (Exception e) {
            logger().error(e.getMessage(), e);
            abort = true;
        }
    }

}
