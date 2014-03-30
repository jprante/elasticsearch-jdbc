
package org.xbib.elasticsearch.river.jdbc.support;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.unit.TimeValue;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.xbib.elasticsearch.river.jdbc.RiverFlow;
import org.xbib.elasticsearch.river.jdbc.RiverMouth;
import org.xbib.elasticsearch.river.jdbc.RiverSource;

import static org.elasticsearch.common.collect.Maps.newHashMap;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * The river context consists of the parameters that span source and mouth settings.
 * It represents the river state, for supporting the river task execution, and river scripting.
 */
public class RiverContext {

    /**
     * The name of the river.
     */
    private String name;
    /**
     * The settings of the river
     */
    private Map<String, Object> settings;

    /**
     * The flow of the river
     */
    private RiverFlow flow;
    /**
     * The source of the river
     */
    private RiverSource source;
    /**
     * The target of the river
     */
    private RiverMouth mouth;

    private String schedule;

    private Integer poolsize;
    /**
     * The polling interval
     */
    private TimeValue interval;
    /**
     * The job name of the current river task
     */
    private String job;
    /**
     * The SQL commands
     */
    private List<SQLCommand> sql;

    /**
     * Autocomit enabled or not
     */
    private boolean autocommit;

    /**
     * The fetch size
     */
    private int fetchSize;

    /**
     * The maximum numbe rof rows per statement execution
     */
    private int maxRows;

    /**
     * The number of retries
     */
    private int retries;

    /**
     * The time to wait between retries
     */
    private TimeValue maxretrywait;

    /**
     * The locale for numerical format
     */
    private Locale locale;

    private int rounding;

    private int scale = -1;

    private String resultSetType = "TYPE_FORWARD_ONLY";

    private String resultSetConcurrency = "CONCUR_UPDATABLE";

    /**
     * Column name that contains creation time (for column strategy)
     */
    private String columnCreatedAt;

    /**
     * Column name that contains last update time (for column strategy)
     */
    private String columnUpdatedAt;

    /**
     * Column name that contains deletion time (for column strategy)
     */
    private String columnDeletedAt;

    /**
     * Columns name should be automatically escaped by proper db quote mark or not (for column strategy)
     */
    private boolean columnEscape;

    public RiverContext riverSettings(Map<String, Object> settings) {
        this.settings = settings;
        return this;
    }

    public Map<String, Object> riverSettings() {
        return settings;
    }

    public RiverContext riverName(String name) {
        this.name = name;
        return this;
    }

    public String riverName() {
        return name;
    }

    public RiverContext riverFlow(RiverFlow flow) {
        this.flow = flow;
        return this;
    }

    public RiverFlow riverFlow() {
        return flow;
    }

    public RiverContext riverSource(RiverSource source) {
        this.source = source;
        return this;
    }

    public RiverSource riverSource() {
        return source;
    }

    public RiverContext riverMouth(RiverMouth mouth) {
        this.mouth = mouth;
        return this;
    }

    public RiverMouth riverMouth() {
        return mouth;
    }

    public RiverContext setLocale(String languageTag) {
        this.locale = LocaleUtil.toLocale(languageTag);
        Locale.setDefault(locale); // for JDBC drivers internals
        return this;
    }

    public Locale getLocale() {
        return locale;
    }

    public RiverContext job(String job) {
        this.job = job;
        return this;
    }

    public String job() {
        return job;
    }

    public RiverContext setInterval(TimeValue interval) {
        this.interval = interval;
        return this;
    }

    public TimeValue getInterval() {
        return interval;
    }

    public RiverContext setSchedule(String schedule) {
        this.schedule = schedule;
        return this;
    }

    public String getSchedule() {
        return schedule;
    }

    public RiverContext setPoolSize(Integer poolsize) {
        this.poolsize = poolsize;
        return this;
    }

    public Integer getPoolSize() {
        return poolsize;
    }

    public RiverContext setStatements(List<SQLCommand> sql) {
        this.sql = sql;
        return this;
    }

    public List<SQLCommand> getStatements() {
        return sql;
    }

    public RiverContext setAutoCommit(boolean autocommit) {
        this.autocommit = autocommit;
        return this;
    }

    public boolean getAutoCommit() {
        return autocommit;
    }

    public RiverContext setFetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
        return this;
    }

    public int getFetchSize() {
        return fetchSize;
    }

    public RiverContext setMaxRows(int maxRows) {
        this.maxRows = maxRows;
        return this;
    }

    public int getMaxRows() {
        return maxRows;
    }

    public RiverContext setRetries(int retries) {
        this.retries = retries;
        return this;
    }

    public int getRetries() {
        return retries;
    }

    public RiverContext setMaxRetryWait(TimeValue maxretrywait) {
        this.maxretrywait = maxretrywait;
        return this;
    }

    public TimeValue getMaxRetryWait() {
        return maxretrywait;
    }

    public RiverContext columnUpdatedAt(String updatedAt) {
        this.columnUpdatedAt = updatedAt;
        return this;
    }

    public RiverContext setRounding(String rounding) {
        if ("ceiling".equalsIgnoreCase(rounding)) {
            this.rounding = BigDecimal.ROUND_CEILING;
        } else if ("down".equalsIgnoreCase(rounding)) {
            this.rounding = BigDecimal.ROUND_DOWN;
        } else if ("floor".equalsIgnoreCase(rounding)) {
            this.rounding = BigDecimal.ROUND_FLOOR;
        } else if ("halfdown".equalsIgnoreCase(rounding)) {
            this.rounding = BigDecimal.ROUND_HALF_DOWN;
        } else if ("halfeven".equalsIgnoreCase(rounding)) {
            this.rounding = BigDecimal.ROUND_HALF_EVEN;
        } else if ("halfup".equalsIgnoreCase(rounding)) {
            this.rounding = BigDecimal.ROUND_HALF_UP;
        } else if ("unnecessary".equalsIgnoreCase(rounding)) {
            this.rounding = BigDecimal.ROUND_UNNECESSARY;
        } else if ("up".equalsIgnoreCase(rounding)) {
            this.rounding = BigDecimal.ROUND_UP;
        }
        return this;
    }

    public int getRounding() {
        return rounding;
    }

    public RiverContext setScale(int scale) {
        this.scale = scale;
        return this;
    }

    public int getScale() {
        return scale;
    }

    public RiverContext setResultSetType(String resultSetType) {
        this.resultSetType = resultSetType;
        return this;
    }

    public String getResultSetType() {
        return resultSetType;
    }

    public RiverContext setResultSetConcurrency(String resultSetConcurrency) {
        this.resultSetConcurrency = resultSetConcurrency;
        return this;
    }

    public String getResultSetConcurrency() {
        return resultSetConcurrency;
    }

    public String columnUpdatedAt() {
        return columnUpdatedAt;
    }

    public RiverContext columnCreatedAt(String createdAt) {
        this.columnCreatedAt = createdAt;
        return this;
    }

    public String columnCreatedAt() {
        return columnCreatedAt;
    }

    public RiverContext columnDeletedAt(String deletedAt) {
        this.columnDeletedAt = deletedAt;
        return this;
    }

    public String columnDeletedAt() {
        return columnDeletedAt;
    }

    public RiverContext columnEscape(boolean escape) {
        this.columnEscape = escape;
        return this;
    }

    public boolean columnEscape() {
        return this.columnEscape;
    }

    public RiverContext contextualize() {
        if (source != null) {
            source.riverContext(this);
        }
        if (mouth != null) {
            mouth.riverContext(this);
        }
        if (flow != null) {
            flow.riverContext(this);
        }
        return this;
    }

    public Map<String,Object> asMap() {
        try {
           XContentBuilder builder = jsonBuilder();
           builder.startObject()
                .field("riverName", name)
                .field("settings", settings)
                .field("locale", LocaleUtil.fromLocale(locale))
                .field("job", job)
                .field("schedule", schedule)
                .field("interval", interval)
                .field("sql", sql)
                .field("autocommit", autocommit)
                .field("fetchsize", fetchSize)
                .field("maxrows", maxRows)
                .field("retries", retries)
                .field("maxretrywait", maxretrywait)
                .field("columnCreatedAt", columnCreatedAt)
                .field("columnUpdatedAt", columnUpdatedAt)
                .field("columnDeletedAt", columnDeletedAt)
                .field("columnEscape", columnEscape)
                .endObject();
            Tuple<XContentType,Map<String,Object>> tuple =XContentHelper.convertToMap(builder.bytes(), true);
            return tuple.v2();
        } catch (IOException e) {
            // does not happen
            return newHashMap();
        }
    }
}
