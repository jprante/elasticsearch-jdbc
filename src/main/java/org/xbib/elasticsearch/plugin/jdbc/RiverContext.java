package org.xbib.elasticsearch.plugin.jdbc;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.xbib.elasticsearch.river.jdbc.RiverFlow;
import org.xbib.elasticsearch.river.jdbc.RiverMouth;
import org.xbib.elasticsearch.river.jdbc.RiverSource;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

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

    private RiverFlow flow;

    /**
     * The source of the river
     */
    private RiverSource source;
    /**
     * The target of the river
     */
    private RiverMouth mouth;

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

    /**
     * Ignore null values from being indexed
     */
    private boolean ignoreNull;

    public RiverContext setRiverSettings(Map<String, Object> settings) {
        this.settings = settings;
        return this;
    }

    public Map<String, Object> getRiverSettings() {
        return settings;
    }

    public RiverContext setRiverName(String name) {
        this.name = name;
        return this;
    }

    public String getRiverName() {
        return name;
    }

    public RiverContext setRiverSource(RiverSource source) {
        this.source = source;
        return this;
    }

    public RiverSource getRiverSource() {
        return source;
    }

    public RiverContext setRiverMouth(RiverMouth mouth) {
        this.mouth = mouth;
        return this;
    }

    public RiverMouth getRiverMouth() {
        return mouth;
    }

    public RiverContext setRiverFlow(RiverFlow flow) {
        this.flow = flow;
        return this;
    }

    public RiverFlow getRiverFlow() {
        return flow;
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

    public RiverContext columnUpdatedAt(String updatedAt) {
        this.columnUpdatedAt = updatedAt;
        return this;
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

    public RiverContext ignoreNull(boolean ignoreNull) {
        this.ignoreNull = ignoreNull;
        return this;
    }

    public boolean ignoreNull() {
        return ignoreNull;
    }

    public RiverContext contextualize() {
        if (source != null) {
            source.setRiverContext(this);
        }
        if (mouth != null) {
            mouth.setRiverContext(this);
        }
        if (flow != null) {
            flow.setRiverContext(this);
        }
        return this;
    }

    public Map<String, Object> asMap() {
        try {
            XContentBuilder builder = jsonBuilder()
                    .startObject()
                    .field("rivername", name)
                    .field("settings", settings)
                    .field("locale", LocaleUtil.fromLocale(locale))
                    .field("job", job)
                    .field("sql", sql)
                    .field("autocommit", autocommit)
                    .field("fetchsize", fetchSize)
                    .field("maxrows", maxRows)
                    .field("retries", retries)
                    .field("maxretrywait", maxretrywait)
                    .field("resultsetconcurrency", resultSetConcurrency)
                    .field("resultsettype", resultSetType)
                    .field("rounding", rounding)
                    .field("scale", scale)
                    .field("columnCreatedAt", columnCreatedAt)
                    .field("columnUpdatedAt", columnUpdatedAt)
                    .field("columnDeletedAt", columnDeletedAt)
                    .field("columnEscape", columnEscape)
                    .field("ignoreNull", ignoreNull)
                    .endObject();
            return XContentHelper.convertToMap(builder.bytes(), true).v2();
        } catch (IOException e) {
            // should really not happen
            return new HashMap<String, Object>();
        }
    }

    @Override
    public String toString() {
        return asMap().toString();
    }
}
