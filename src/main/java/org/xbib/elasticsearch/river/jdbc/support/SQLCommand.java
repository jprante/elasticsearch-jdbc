
package org.xbib.elasticsearch.river.jdbc.support;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.xcontent.support.XContentMapValues;

import static org.elasticsearch.common.collect.Lists.newLinkedList;
import static org.elasticsearch.common.collect.Maps.newHashMap;

public class SQLCommand {

    private final static ESLogger logger = ESLoggerFactory.getLogger(SQLCommand.class.getName());

    private String sql;

    private List<? super Object> params = newLinkedList();

    private Map<String,Object> results = newHashMap();

    private boolean callable;

    public SQLCommand setSQL(String sql) throws IOException {
        if (sql.endsWith(".sql")) {
            Reader r = new InputStreamReader(new FileInputStream(sql), "UTF-8");
            sql = Streams.copyToString(r);
            r.close();
        }
        this.sql = sql;
        return this;
    }

    public String getSQL() {
        return sql;
    }

    public SQLCommand setParameters(List<? super Object> params) {
        this.params = params;
        return this;
    }

    public List<? super Object> getParameters() {
        return params;
    }

    public SQLCommand setCallable(boolean callable) {
        this.callable = callable;
        return this;
    }

    public boolean isCallable() {
        return callable;
    }

    public boolean isQuery() {
        int p1 = sql.toLowerCase().indexOf("select");
        if (p1 < 0) {
            return false;
        }
        int p2 = sql.toLowerCase().indexOf("update");
        if (p2 < 0) {
            return true;
        }
        int p3 = sql.toLowerCase().indexOf("insert");
        return p3 < 0 || p1 < p2 && p1 < p3;
    }

    public void setResults(Map<String,Object> results) {
        this.results = results;
    }

    public Map<String,Object> getResults() {
        return results;
    }

    public static List<SQLCommand> parse(Map<String,Object> settings) {
        List<SQLCommand> sql = newLinkedList();
        if (!XContentMapValues.isArray(settings.get("sql"))) {
            settings.put("sql", Arrays.asList(settings.get("sql")));
        }
        List<? super Object> list = (List<? super Object>) settings.get("sql");
        for (Object entry : list) {
            SQLCommand command = new SQLCommand();
            try {
                if (entry instanceof Map) {
                    Map<String, Object> m = (Map<String, Object>) entry;
                    if (m.containsKey("statement")) {
                        command.setSQL((String) m.get("statement"));
                    }
                    if (m.containsKey("parameter")) {
                        command.setParameters(XContentMapValues.extractRawValues("parameter", m));
                    }
                    if (m.containsKey("callable")) {
                        command.setCallable(XContentMapValues.nodeBooleanValue(m.get("callable")));
                    }
                    if (m.containsKey("register")) {
                        command.setResults(XContentMapValues.nodeMapValue(m.get("register"), null));
                    }
                } else if (entry instanceof String) {
                    command.setSQL((String) entry);
                }
                sql.add(command);
            } catch (IOException e) {
                logger.warn("SQL command not found", e);
            }
        }
        return sql;
    }

    public String toString() {
        return "statement=" + sql + " parameter=" + params + " callable=" + callable;
    }

}
