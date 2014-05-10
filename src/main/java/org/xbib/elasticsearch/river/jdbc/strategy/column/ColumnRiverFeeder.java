
package org.xbib.elasticsearch.river.jdbc.strategy.column;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.xbib.elasticsearch.action.river.state.RiverState;
import org.xbib.elasticsearch.plugin.feeder.jdbc.JDBCFeeder;
import org.xbib.pipeline.Pipeline;
import org.xbib.pipeline.PipelineRequest;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class ColumnRiverFeeder<T, R extends PipelineRequest, P extends Pipeline<T, R>>
        extends JDBCFeeder<T,R,P> {

    private final static ESLogger logger = ESLoggerFactory.getLogger(ColumnRiverFeeder.class.getSimpleName());

    @Override
    protected void createRiverContext(String riverType, String riverName, Map<String, Object> mySettings) {
        super.createRiverContext(riverType, riverName, mySettings);
        // defaults for column strategy
        String columnCreatedAt = XContentMapValues.nodeStringValue(mySettings.get("created_at"), "created_at");
        String columnUpdatedAt = XContentMapValues.nodeStringValue(mySettings.get("updated_at"), "updated_at");
        String columnDeletedAt = XContentMapValues.nodeStringValue(mySettings.get("deleted_at"), null);
        boolean columnEscape = XContentMapValues.nodeBooleanValue(mySettings.get("column_escape"), true);
        riverContext
                .columnCreatedAt(columnCreatedAt)
                .columnUpdatedAt(columnUpdatedAt)
                .columnDeletedAt(columnDeletedAt)
                .columnEscape(columnEscape);
    }

        @Override
    public void executeTask(Map<String, Object> map) throws Exception {
        logger.info("processing map {}", map);
        createRiverContext("jdbc", "feeder", map);
        if (riverState == null) {
            riverState = new RiverState();
        }
        riverState.load(ingest.client());
        // increment state counter
        Long counter = riverState.getCounter() + 1;
        this.riverState = riverState.setCounter(counter)
                .setEnabled(true)
                .setActive(true)
                .setTimestamp(new Date());
        riverState.save(ingest.client());
        if (logger.isDebugEnabled()) {
            logger.debug("state saved before fetch");
        }
        // set the job number to the state counter
        riverContext.job(Long.toString(counter));
        if (logger.isDebugEnabled()) {
            logger.debug("trying to fetch ...");
        }

        // column river specific code here
        TimeValue lastRunTime = readLastRunTimeFromCustomInfo();
        TimeValue currentTime = new TimeValue(new java.util.Date().getTime());
        writeTimesToJdbcSettings(lastRunTime, currentTime);
        riverContext.getRiverSource().fetch();
        writeCustomInfo(currentTime.millis());


        if (logger.isDebugEnabled()) {
            logger.debug("fetched, flushing");
        }
        riverContext.getRiverMouth().flush();
        if (logger.isDebugEnabled()) {
            logger.debug("flushed");
        }
        riverContext.getRiverSource().closeReading();
        riverContext.getRiverSource().closeWriting();
        this.riverState = riverState
                .setActive(false)
                .setTimestamp(new Date());
        riverState.save(ingest.client());
    }

    private TimeValue readLastRunTimeFromCustomInfo() throws IOException {
        try {
            GetResponse response = getClient().prepareGet("_river", riverContext.getRiverName(), ColumnRiverFlow.DOCUMENT).execute().actionGet();
            if (response != null && response.isExists()) {
                Map jdbcState = (Map) response.getSourceAsMap().get("jdbc");

                if (jdbcState != null) {
                    Number lastRunTime = (Number) jdbcState.get(ColumnRiverFlow.LAST_RUN_TIME);

                    if (lastRunTime != null) {
                        return new TimeValue(lastRunTime.longValue());
                    }
                } else {
                    throw new IOException("can't retrieve previously persisted state from _river/" + riverContext.getRiverName());
                }
            }
        } catch (IndexMissingException e) {
            logger.warn("river state missing: _river/{}/{}", riverContext.getRiverName(), "_custom");
        }

        return null;
    }

    private void writeCustomInfo(long lastRunAt) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("jdbc")
                .field(ColumnRiverFlow.LAST_RUN_TIME, lastRunAt)
                .endObject()
                .endObject();
        getClient().prepareBulk().add(Requests.indexRequest("_river").type(riverContext.getRiverName()).id(ColumnRiverFlow.DOCUMENT)
                .source(builder.string())).execute().actionGet();
    }

    @SuppressWarnings({"unchecked"})
    private void writeTimesToJdbcSettings(TimeValue lastRunTime, TimeValue currentTime) {
        if (riverContext == null || riverContext.getRiverSettings() == null) {
            return;
        }
        Map<String, Object> jdbcSettings = (Map<String, Object>) riverContext.getRiverSettings().get("jdbc");
        if (jdbcSettings == null) {
            jdbcSettings = new HashMap<String, Object>();
            riverContext.getRiverSettings().put("jdbc", jdbcSettings);
        }
        jdbcSettings.put(ColumnRiverFlow.LAST_RUN_TIME, lastRunTime);
        jdbcSettings.put(ColumnRiverFlow.CURRENT_RUN_STARTED_TIME, currentTime);
    }
}
