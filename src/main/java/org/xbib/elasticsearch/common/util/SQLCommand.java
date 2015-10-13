/*
 * Copyright (C) 2014 Jörg Prante
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.xbib.elasticsearch.common.util;

import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.xcontent.support.XContentMapValues;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * The SQL command
 */
public class SQLCommand {

    private String sql;

    private static final Pattern STATEMENT_PATTERN = Pattern.compile("^\\s*(update|insert)", Pattern.CASE_INSENSITIVE);

    private List<Object> params = new LinkedList<>();

    private boolean write;

    private Map<String, Object> register = new HashMap<>();

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

    public SQLCommand setParameters(List<Object> params) {
        this.params = params;
        return this;
    }

    public List<Object> getParameters() {
        return params;
    }

    public SQLCommand setCallable(boolean callable) {
        this.callable = callable;
        return this;
    }

    public boolean isCallable() {
        return callable;
    }

    public SQLCommand setWrite(boolean write) {
        this.write = write;
        return this;
    }

    public boolean isWrite() {
        return write;
    }

    public boolean isQuery() {
        if (sql == null) {
            throw new IllegalArgumentException("no SQL found");
        }
        if (write) {
            return false;
        }
        if (STATEMENT_PATTERN.matcher(sql).find()) {
            return false;
        }
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

    /**
     * A register is for parameters of a callable statement.
     *
     * @param register a map for registering parameters
     */
    public void setRegister(Map<String, Object> register) {
        this.register = register;
    }

    /**
     * Get the parameters of a callable statement
     *
     * @return the register map
     */
    public Map<String, Object> getRegister() {
        return register;
    }

    @SuppressWarnings({"unchecked"})
    public static List<SQLCommand> parse(Map<String, Object> settings) {
        List<SQLCommand> sql = new LinkedList<SQLCommand>();
        if (!XContentMapValues.isArray(settings.get("sql"))) {
            settings.put("sql", Arrays.asList(settings.get("sql")));
        }
        List<Object> list = (List<Object>) settings.get("sql");
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
                    if (m.containsKey("write")) {
                        command.setWrite(XContentMapValues.nodeBooleanValue(m.get("write")));
                    }
                    if (m.containsKey("callable")) {
                        command.setCallable(XContentMapValues.nodeBooleanValue(m.get("callable")));
                    }
                    if (m.containsKey("register")) {
                        command.setRegister(XContentMapValues.nodeMapValue(m.get("register"), null));
                    }
                } else if (entry instanceof String) {
                    command.setSQL((String) entry);
                }
                sql.add(command);
            } catch (IOException e) {
                throw new IllegalArgumentException("SQL command not found", e);
            }
        }
        return sql;
    }

    public String toString() {
        return "statement=" + sql + " parameter=" + params + " write=" + write + " callable=" + callable;
    }

}
