package org.xbib.elasticsearch.support.client;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.util.Map;

public abstract class BaseIngestTransportClient extends BaseTransportClient
        implements Ingest {

    private final static ESLogger logger = ESLoggerFactory.getLogger(BaseIngestTransportClient.class.getSimpleName());

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
        if (client == null) {
            logger.warn("no client for create index");
            return this;
        }
        if (index == null) {
            logger.warn("no index name given to create index");
            return this;
        }
        CreateIndexRequest request = new CreateIndexRequest(index).listenerThreaded(false);
        if (getSettings() != null) {
            request.settings(getSettings());
        }
        if (getMappings() != null) {
            for (Map.Entry<String, String> me : getMappings().entrySet()) {
                request.mapping(me.getKey(), me.getValue());
            }
        }
        logger.info("creating index {} with settings = {}, mappings = {}",
                index, getSettings() != null ? getSettings().getAsMap() : null, getMappings());
        try {
            client.admin().indices().create(request).actionGet();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        logger.info("index {} created", index);
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
        try {
            client.admin().indices().delete(new DeleteIndexRequest(index)).actionGet();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
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
