/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.river.jdbc.support;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.river.jdbc.RiverMouth;
import org.elasticsearch.river.jdbc.RiverSource;

import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * The river context consists of the parameters that span over source and target, and the source and target.
 * It represents the river state, for supporting the river task, and river scripting.
 *
 * @author Jörg Prante <joergprante@gmail.com>
 */
public class RiverContext {

    /**
     * The name of the river.
     */
    private String name;
    /**
     * The river index name
     */
    private String riverIndexName;
    /**
     * The settings of the river
     */
    private Map<String, Object> settings;
    /**
     * The source of the river
     */
    private RiverSource source;
    /**
     * The target of the river
     */
    private RiverMouth mouth;
    /**
     * The polling interval
     */
    private TimeValue poll;

    /**
     * The job name of the current river task
     */
    private String job;
    /**
     * The SQL statement
     */
    private String sql;
    /**
     * Parameters for the SQL statement
     */
    private List<? extends Object> sqlparams;
    /**
     * The acknowledge SQL statement
     */
    private String acksql;
    /**
     * Parameters for the acknowledge SQL statement
     */
    private List<? extends Object> acksqlparams;
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

    /**
     * If digesting should be used or not
     */
    private boolean digesting;

    public RiverContext riverSettings(Map<String, Object> settings) {
        this.settings = settings;
        return this;
    }

    public Map<String, Object> riverSettings() {
        return settings;
    }

    public RiverContext riverIndexName(String name) {
        this.riverIndexName = name;
        return this;
    }

    public String riverIndexName() {
        return riverIndexName;
    }

    public RiverContext riverName(String name) {
        this.name = name;
        return this;
    }

    public String riverName() {
        return name;
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

    public RiverContext job(String job) {
        this.job = job;
        return this;
    }

    public String job() {
        return job;
    }

    public RiverContext pollInterval(TimeValue poll) {
        this.poll = poll;
        return this;
    }

    public TimeValue pollingInterval() {
        return poll;
    }

    public RiverContext pollStatement(String sql) {
        this.sql = sql;
        return this;
    }

    public String pollStatement() {
        return sql;
    }

    public RiverContext pollStatementParams(List<? extends Object> params) {
        this.sqlparams = params;
        return this;
    }

    public List<? extends Object> pollStatementParams() {
        return sqlparams;
    }

    public RiverContext pollAckStatement(String acksql) {
        this.acksql = acksql;
        return this;
    }

    public String pollAckStatement() {
        return acksql;
    }

    public RiverContext pollAckStatementParams(List<? extends Object> params) {
        this.acksqlparams = params;
        return this;
    }

    public List<? extends Object> pollAckStatementParams() {
        return acksqlparams;
    }

    public RiverContext autocommit(boolean enabled) {
        this.autocommit = enabled;
        return this;
    }

    public boolean autocommit() {
        return autocommit;
    }

    public RiverContext fetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
        return this;
    }

    public int fetchSize() {
        return fetchSize;
    }

    public RiverContext maxRows(int maxRows) {
        this.maxRows = maxRows;
        return this;
    }

    public int maxRows() {
        return maxRows;
    }

    public RiverContext retries(int retries) {
        this.retries = retries;
        return this;
    }

    public int retries() {
        return retries;
    }

    public RiverContext maxRetryWait(TimeValue maxretrywait) {
        this.maxretrywait = maxretrywait;
        return this;
    }

    public TimeValue maxRetryWait() {
        return maxretrywait;
    }

    public RiverContext locale(String languageTag) {
        this.locale = LocaleUtil.toLocale(languageTag);
        Locale.setDefault(locale); // for JDBC drivers internals
        return this;
    }

    public Locale locale() {
        return locale;
    }

    public RiverContext digesting(boolean digesting) {
        this.digesting = digesting;
        return this;
    }

    public boolean digesting() {
        return digesting;
    }

    public RiverContext contextualize() {
        if (source != null) {
            source.riverContext(this);
        }
        if (mouth != null) {
            mouth.riverContext(this);
        }
        return this;
    }
}
