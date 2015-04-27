/*
 * Copyright (C) 2015 Jörg Prante
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
package org.xbib.tools;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequestBuilder;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.common.hppc.cursors.ObjectCursor;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.joda.time.format.DateTimeFormat;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class TimewindowFeeder extends Feeder {

    private final static Logger logger = LogManager.getLogger(TimewindowFeeder.class);

    private static String index;

    private static String concreteIndex;

    protected void setIndex(String index) {
        this.index = index;
    }

    protected String getIndex() {
        return index;
    }

    protected void setConcreteIndex(String concreteIndex) {
        this.concreteIndex = concreteIndex;
    }

    protected String getConcreteIndex() {
        return concreteIndex;
    }

    @Override
    protected void prepare() throws IOException {
        if (ingest == null) {
            Integer maxbulkactions = settings.getAsInt("maxbulkactions", 1000);
            Integer maxconcurrentbulkrequests = settings.getAsInt("maxconcurrentbulkrequests",
                    Runtime.getRuntime().availableProcessors());
            ingest = createIngest();
            ingest.maxActionsPerBulkRequest(maxbulkactions)
                    .maxConcurrentBulkRequests(maxconcurrentbulkrequests);
        }
        String timeWindow = settings.get("timewindow") != null ?
                DateTimeFormat.forPattern(settings.get("timewindow")).print(new DateTime()) : "";
        setConcreteIndex(resolveAlias(getIndex() + timeWindow));
        Pattern pattern = Pattern.compile("^(.*?)\\d+$");
        Matcher m = pattern.matcher(getConcreteIndex());
        setIndex(m.matches() ? m.group(1) : getConcreteIndex());
        logger.info("base index name = {}, concrete index name = {}", getIndex(), getConcreteIndex());
        super.prepare();
    }

    @Override
    protected TimewindowFeeder createIndex(String index) throws IOException {
        /*ingest.newClient(ImmutableSettings.settingsBuilder()
                .put("cluster.name", settings.get("elasticsearch.cluster"))
                .putArray("host", settings.getAsArray("elasticsearch.host"))
                .put("port", settings.getAsInt("elasticsearch.port", 9300))
                .put("sniff", settings.getAsBoolean("elasticsearch.sniff", false))
                .build());*/
        if (ingest.client() != null) {
            ingest.waitForCluster(ClusterHealthStatus.YELLOW, TimeValue.timeValueSeconds(30));
            if (settings.getAsBoolean("onlyalias", false)) {
                updateAliases();
                return this;
            }
            try {
                if (settings.getAsStructuredMap().containsKey("index_settings")) {
                    String indexSettings = settings.get("index_settings");
                    InputStream indexSettingsInput = (indexSettings.startsWith("classpath:") ?
                            new URL(null, indexSettings, new ClasspathURLStreamHandler()) :
                            new URL(indexSettings)).openStream();
                    String indexMappings = settings.get("type_mapping", null);
                    InputStream indexMappingsInput = (indexMappings.startsWith("classpath:") ?
                            new URL(null, indexMappings, new ClasspathURLStreamHandler()) :
                            new URL(indexMappings)).openStream();
                    ingest.newIndex(getConcreteIndex(), getType(),
                            indexSettingsInput, indexMappingsInput);
                    indexSettingsInput.close();
                    indexMappingsInput.close();
                    ingest.startBulk(getConcreteIndex(), -1, 1000);
                }
            } catch (Exception e) {
                if (!settings.getAsBoolean("ignoreindexcreationerror", false)) {
                    throw e;
                } else {
                    logger.warn("index creation error, but configured to ignore", e);
                }
            }
        }
        return this;
    }

    protected String resolveAlias(String alias) {
        if (ingest.client() == null) {
            return alias;
        }
        GetAliasesResponse getAliasesResponse = ingest.client().admin().indices().prepareGetAliases(alias).execute().actionGet();
        if (!getAliasesResponse.getAliases().isEmpty()) {
            return getAliasesResponse.getAliases().keys().iterator().next().value;
        }
        return alias;
    }

    protected void updateAliases() {
        if (ingest.client() == null) {
            return;
        }
        String index = getIndex();
        String concreteIndex = getConcreteIndex();
        if (!index.equals(concreteIndex)) {
            IndicesAliasesRequestBuilder requestBuilder = ingest.client().admin().indices().prepareAliases();
            GetAliasesResponse getAliasesResponse = ingest.client().admin().indices().prepareGetAliases(index).execute().actionGet();
            if (getAliasesResponse.getAliases().isEmpty()) {
                logger.info("adding alias {} to index {}", index, concreteIndex);
                requestBuilder.addAlias(concreteIndex, index);
                // identifier is alias
                if (settings.get("identifier") != null) {
                    requestBuilder.addAlias(concreteIndex, settings.get("identifier"));
                }
            } else {
                for (ObjectCursor<String> indexName : getAliasesResponse.getAliases().keys()) {
                    if (indexName.value.startsWith(index)) {
                        logger.info("switching alias {} from index {} to index {}", index, indexName.value, concreteIndex);
                        requestBuilder.removeAlias(indexName.value, index)
                                .addAlias(concreteIndex, index);
                        if (settings.get("identifier") != null) {
                            requestBuilder.removeAlias(indexName.value, settings.get("identifier"))
                                    .addAlias(concreteIndex, settings.get("identifier"));
                        }
                    }
                }
            }
            requestBuilder.execute().actionGet();
            if (settings.getAsBoolean("retention.enabled", false)) {
                performRetentionPolicy(
                        getIndex(),
                        getConcreteIndex(),
                        settings.getAsInt("retention.diff", 48),
                        settings.getAsInt("retention.mintokeep", 2));
            }
        }
    }

    public void performRetentionPolicy(String index, String concreteIndex, int timestampdiff, int mintokeep) {
        if (ingest.client() == null) {
            return;
        }
        if (index.equals(concreteIndex)) {
            return;
        }
        GetIndexResponse getIndexResponse = ingest.client().admin().indices()
                .prepareGetIndex()
                .execute().actionGet();
        Pattern pattern = Pattern.compile("^(.*?)(\\d+)$");
        List<String> indices = new ArrayList<>();
        logger.info("{} indices", getIndexResponse.getIndices().length);
        for (String s : getIndexResponse.getIndices()) {
            Matcher m = pattern.matcher(s);
            if (m.matches()) {
                if (index.equals(m.group(1)) && !s.equals(concreteIndex)) {
                    indices.add(s);
                }
            }
        }
        if (indices.isEmpty()) {
            logger.info("no indices found, retention policy skipped");
            return;
        }
        if (mintokeep > 0 && indices.size() < mintokeep) {
            logger.info("{} indices found, not enough for retention policy ({}),  skipped",
                    indices.size(), mintokeep);
            return;
        } else {
            logger.info("candidates for deletion = {}", indices);
        }
        List<String> indicesToDelete = new ArrayList<String>();
        // our index
        Matcher m1 = pattern.matcher(concreteIndex);
        if (m1.matches()) {
            Integer i1 = Integer.parseInt(m1.group(2));
            for (String s : indices) {
                Matcher m2 = pattern.matcher(s);
                if (m2.matches()) {
                    Integer i2 = Integer.parseInt(m2.group(2));
                    int kept = 1 + indices.size() - indicesToDelete.size();
                    if (timestampdiff > 0 && i1 - i2 > timestampdiff && mintokeep <= kept) {
                        indicesToDelete.add(s);
                    }
                }
            }
        }
        logger.info("indices to delete = {}", indicesToDelete);
        if (indicesToDelete.isEmpty()) {
            logger.info("not enough indices found to delete, retention policy complete");
            return;
        }
        String[] s = indicesToDelete.toArray(new String[indicesToDelete.size()]);
        DeleteIndexRequestBuilder requestBuilder = ingest.client().admin().indices().prepareDelete(s);
        DeleteIndexResponse response = requestBuilder.execute().actionGet();
        if (!response.isAcknowledged()) {
            logger.warn("retention delete index operation was not acknowledged");
        }
    }

}
