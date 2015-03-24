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

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequestBuilder;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.common.collect.Lists.newLinkedList;

public class ClientHelper {

    public static List<String> getConnectedNodes(TransportClient client) {
        List<String> nodes = newLinkedList();
        if (client.connectedNodes() != null) {
            for (DiscoveryNode discoveryNode : client.connectedNodes()) {
                nodes.add(discoveryNode.toString());
            }
        }
        return nodes;
    }

    public static void updateIndexSetting(Client client, String index, String key, Object value) throws IOException {
        if (client == null) {
            throw new IOException("no client");
        }
        if (index == null) {
            throw new IOException("no index name given");
        }
        if (key == null) {
            throw new IOException("no key given");
        }
        if (value == null) {
            throw new IOException("no value given");
        }
        ImmutableSettings.Builder settingsBuilder = ImmutableSettings.settingsBuilder();
        settingsBuilder.put(key, value.toString());
        UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(index)
                .settings(settingsBuilder);
        client.admin().indices().updateSettings(updateSettingsRequest).actionGet();
    }

    public static int waitForRecovery(Client client, String index) throws IOException {
        if (index == null) {
            throw new IOException("unable to waitfor recovery, index not set");
        }
        RecoveryResponse response = client.admin().indices().prepareRecoveries(index).execute().actionGet();
        int shards = response.getTotalShards();
        client.admin().cluster().prepareHealth(index).setWaitForActiveShards(shards).execute().actionGet();
        return shards;
    }

    public static void waitForCluster(Client client, ClusterHealthStatus status, TimeValue timeout) throws IOException {
        try {
            ClusterHealthResponse healthResponse =
                    client.admin().cluster().prepareHealth().setWaitForStatus(status).setTimeout(timeout).execute().actionGet();
            if (healthResponse != null && healthResponse.isTimedOut()) {
                throw new IOException("cluster state is " + healthResponse.getStatus().name()
                        + " and not " + status.name()
                        + ", cowardly refusing to continue with operations");
            }
        } catch (ElasticsearchTimeoutException e) {
            throw new IOException("timeout, cluster does not respond to health request, cowardly refusing to continue with operations");
        }
    }

    public static String clusterName(Client client) {
        try {
            ClusterStateRequestBuilder clusterStateRequestBuilder =
                    new ClusterStateRequestBuilder(client.admin().cluster()).all();
            ClusterStateResponse clusterStateResponse = clusterStateRequestBuilder.execute().actionGet();
            String name = clusterStateResponse.getClusterName().value();
            int nodeCount = clusterStateResponse.getState().getNodes().size();
            return name + " (" + nodeCount + " nodes connected)";
        } catch (ElasticsearchTimeoutException e) {
            return "TIMEOUT";
        } catch (NoNodeAvailableException e) {
            return "DISCONNECTED";
        } catch (Throwable t) {
            return "[" + t.getMessage() + "]";
        }
    }

    public static String healthColor(Client client) {
        try {
            ClusterHealthResponse healthResponse =
                    client.admin().cluster().prepareHealth().setTimeout(TimeValue.timeValueSeconds(30)).execute().actionGet();
            ClusterHealthStatus status = healthResponse.getStatus();
            return status.name();
        } catch (ElasticsearchTimeoutException e) {
            return "TIMEOUT";
        } catch (NoNodeAvailableException e) {
            return "DISCONNECTED";
        } catch (Throwable t) {
            return "[" + t.getMessage() + "]";
        }
    }

    public static int updateReplicaLevel(Client client, String index, int level) throws IOException {
        waitForCluster(client, ClusterHealthStatus.YELLOW, TimeValue.timeValueSeconds(30));
        updateIndexSetting(client, index, "number_of_replicas", level);
        return waitForRecovery(client, index);
    }

    public static void flush(Client client, String index) {
        client.admin().indices().prepareFlush().setIndices(index).execute().actionGet();
    }

    public static void refresh(Client client, String index) {
        client.admin().indices().prepareRefresh().setIndices(index).execute().actionGet();
    }

}
