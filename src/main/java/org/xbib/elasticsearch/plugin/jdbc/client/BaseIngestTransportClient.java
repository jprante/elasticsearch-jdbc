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
package org.xbib.elasticsearch.plugin.jdbc.client;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

public abstract class BaseIngestTransportClient extends BaseTransportClient
        implements Ingest {

    private final static ESLogger logger = ESLoggerFactory.getLogger("");

    public Ingest newClient(Settings settings) throws IOException {
        super.createClient(settings);
        return this;
    }

    @Override
    public BaseIngestTransportClient shards(int shards) {
        super.addSetting("index.number_of_shards", shards);
        return this;
    }

    @Override
    public BaseIngestTransportClient replica(int replica) {
        super.addSetting("index.number_of_replicas", replica);
        return this;
    }

    @Override
    public BaseIngestTransportClient newIndex(String index) {
        return newIndex(index, null, null);
    }

    @Override
    public BaseIngestTransportClient newIndex(String index, String type, InputStream settings, InputStream mappings) throws IOException {
        configHelper.reset();
        configHelper.setting(settings);
        configHelper.mapping(type, mappings);
        return newIndex(index, configHelper.settings(), configHelper.mappings());
    }

    @Override
    public BaseIngestTransportClient newIndex(String index, Settings settings, Map<String, String> mappings) {
        if (client == null) {
            logger.warn("no client for create index");
            return this;
        }
        if (index == null) {
            logger.warn("no index name given to create index");
            return this;
        }
        CreateIndexRequestBuilder createIndexRequestBuilder =
                new CreateIndexRequestBuilder(client.admin().indices()).setIndex(index);
        Settings concreteSettings = null;
        if (settings != null) {
            concreteSettings = settings;
        } else if (getSettings() != null) {
            concreteSettings = getSettings();
        }
        if (concreteSettings != null) {
            createIndexRequestBuilder.setSettings(concreteSettings);
        }
        if (mappings == null && getMappings() != null) {
            for (String type : getMappings().keySet()) {
                createIndexRequestBuilder.addMapping(type, getMappings().get(type));
            }
        } else if (mappings != null) {
            for (String type : mappings.keySet()) {
                createIndexRequestBuilder.addMapping(type, mappings.get(type));
            }
        }
        createIndexRequestBuilder.execute().actionGet();
        logger.info("index {} created with settings {} and {} mappings", index,
                concreteSettings != null ? concreteSettings.getAsMap() : "",
                mappings != null ? mappings.size() : 0);
        return this;
    }

    @Override
    public synchronized BaseIngestTransportClient deleteIndex(String index) {
        if (client == null) {
            logger.warn("no client for delete index");
            return this;
        }
        if (index == null) {
            logger.warn("no index name given to delete index");
            return this;
        }
        DeleteIndexRequestBuilder deleteIndexRequestBuilder =
                new DeleteIndexRequestBuilder(client.admin().indices(), index);
        deleteIndexRequestBuilder.execute().actionGet();
        return this;
    }

    public BaseIngestTransportClient putMapping(String index) {
        if (client == null) {
            logger.warn("no client for put mapping");
            return this;
        }
        configHelper.putMapping(client, index);
        return this;
    }

    public BaseIngestTransportClient deleteMapping(String index, String type) {
        if (client == null) {
            logger.warn("no client for delete mapping");
            return this;
        }
        configHelper.deleteMapping(client, index, type);
        return this;
    }

    @Override
    public BaseIngestTransportClient waitForCluster(ClusterHealthStatus status, TimeValue timeValue) throws IOException {
        ClientHelper.waitForCluster(client, status, timeValue);
        return this;
    }

}
