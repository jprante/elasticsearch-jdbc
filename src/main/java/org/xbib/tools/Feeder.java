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
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.xbib.elasticsearch.support.client.Ingest;
import org.xbib.elasticsearch.support.client.transport.BulkTransportClient;
import org.xbib.elasticsearch.support.client.ingest.IngestTransportClient;
import org.xbib.elasticsearch.support.client.mock.MockTransportClient;
import org.xbib.pipeline.Pipeline;
import org.xbib.pipeline.PipelineRequest;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

public abstract class Feeder<T, R extends PipelineRequest, P extends Pipeline<T, R>>
        extends Converter<T, R, P> {

    private final static Logger logger = LogManager.getLogger("feeder");

    protected static Ingest ingest;

    private static String index;

    private static String concreteIndex;

    protected String getType() {
        return settings.get("type");
    }
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
    protected Ingest createIngest() {
        return settings.getAsBoolean("mock", false) ? new MockTransportClient() :
                "ingest".equals(settings.get("client")) ? new IngestTransportClient() :
                        new BulkTransportClient();
    }

    @Override
    protected void prepare() throws IOException {
        super.prepare();
        if (ingest == null) {
            Integer maxbulkactions = settings.getAsInt("maxbulkactions", 1000);
            Integer maxconcurrentbulkrequests = settings.getAsInt("maxconcurrentbulkrequests",
                    Runtime.getRuntime().availableProcessors());
            ingest = createIngest();
            ingest.maxActionsPerBulkRequest(maxbulkactions)
                    .maxConcurrentBulkRequests(maxconcurrentbulkrequests);
        }
        createIndex(getIndex());
    }

    @Override
    protected Feeder<T, R, P> cleanup() throws IOException {
        super.cleanup();
        if (ingest != null) {
            try {
                logger.debug("flush");
                ingest.flushIngest();
                logger.info("waiting for completing all bulk responses");
                ingest.waitForResponses(TimeValue.timeValueSeconds(120));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error(e.getMessage(), e);
            }
            ingest.shutdown();
            logger.info("complete");
        }
        return this;
    }

    protected Feeder createIndex(String index) throws IOException {
        if (index == null) {
            return this;
        }
        if (ingest.client() != null) {
            ingest.waitForCluster(ClusterHealthStatus.YELLOW, TimeValue.timeValueSeconds(30));
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
}
