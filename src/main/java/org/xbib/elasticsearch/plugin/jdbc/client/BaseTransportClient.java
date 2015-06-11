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

import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.xbib.elasticsearch.plugin.jdbc.network.NetworkUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public abstract class BaseTransportClient {

    private final static ESLogger logger = ESLoggerFactory.getLogger(BaseTransportClient.class.getSimpleName());

    protected TransportClient client;

    protected ConfigHelper configHelper = new ConfigHelper();

    private boolean isShutdown;

    protected void createClient(Settings settings) throws IOException {
        if (client != null) {
            logger.warn("client is open, closing...");
            client.close();
            client.threadPool().shutdown();
            logger.warn("client is closed");
            client = null;
        }
        if (settings != null) {
            logger.info("creating transport client, java version {}, effective settings {}",
                    System.getProperty("java.version"), settings.getAsMap());
            // false = do not load config settings from environment
            this.client = new TransportClient(settings, false);
            Collection<InetSocketTransportAddress> addrs = findAddresses(settings);
            if (!connect(addrs, settings.getAsBoolean("autodiscover", false))) {
                throw new NoNodeAvailableException("no cluster nodes available, check settings "
                        + settings.getAsMap());
            }
        }
    }

    public Client client() {
        return client;
    }

    public synchronized void shutdown() {
        if (client != null) {
            logger.debug("shutdown started");
            client.close();
            client.threadPool().shutdown();
            client = null;
            logger.debug("shutdown complete");
        }
        isShutdown = true;
    }

    public boolean isShutdown() {
        return isShutdown;
    }

    protected Settings findSettings() {
        ImmutableSettings.Builder settingsBuilder = ImmutableSettings.settingsBuilder();
        settingsBuilder.put("host", "localhost");
        try {
            String hostname = InetAddress.getLocalHost().getHostName();
            logger.debug("the hostname is {}", hostname);
            settingsBuilder.put("host", hostname)
                    .put("port", 9300);
        } catch (UnknownHostException e) {
            logger.warn("can't resolve host name, probably something wrong with network config: " + e.getMessage(), e);
        } catch (Exception e) {
            logger.warn(e.getMessage(), e);
        }
        return settingsBuilder.build();
    }

    protected Collection<InetSocketTransportAddress> findAddresses(Settings settings) throws IOException {
        String[] hostnames = settings.getAsArray("host", new String[]{"localhost"});
        int port = settings.getAsInt("port", 9300);
        Collection<InetSocketTransportAddress> addresses = new ArrayList<>();
        for (String hostname : hostnames) {
            String[] splitHost = hostname.split(":", 2);
            if (splitHost.length == 2) {
                String host = splitHost[0];
                InetAddress inetAddress = NetworkUtils.resolveInetAddress(host, null);
                try {
                    port = Integer.parseInt(splitHost[1]);
                } catch (Exception e) {
                    // ignore
                }
                addresses.add(new InetSocketTransportAddress(inetAddress, port));
            }
            if (splitHost.length == 1) {
                String host = splitHost[0];
                InetAddress inetAddress = NetworkUtils.resolveInetAddress(host, null);
                addresses.add(new InetSocketTransportAddress(inetAddress, port));
            }
        }
        return addresses;
    }

    protected boolean connect(Collection<InetSocketTransportAddress> addresses, boolean autodiscover) {
        logger.info("trying to connect to {}", addresses);
        for (InetSocketTransportAddress address : addresses) {
            client.addTransportAddress(address);
        }
        if (client.connectedNodes() != null) {
            List<DiscoveryNode> nodes = client.connectedNodes().asList();
            if (!nodes.isEmpty()) {
                logger.info("connected to {}", nodes);
                if (autodiscover) {
                    logger.info("trying to auto-discover all cluster nodes...");
                    ClusterStateResponse clusterStateResponse = client.admin().cluster().state(new ClusterStateRequest()).actionGet();
                    DiscoveryNodes discoveryNodes = clusterStateResponse.getState().getNodes();
                    for (DiscoveryNode node : discoveryNodes) {
                        logger.info("connecting to auto-discovered node {}", node);
                        try {
                            client.addTransportAddress(node.address());
                        } catch (Exception e) {
                            logger.warn("can't connect to node " + node, e);
                        }
                    }
                    logger.info("after auto-discovery connected to {}", client.connectedNodes().asList());
                }
                return true;
            }
            return false;
        }
        return false;
    }

    public ImmutableSettings.Builder getSettingsBuilder() {
        return configHelper.settingsBuilder();
    }

    public void resetSettings() {
        configHelper.reset();
    }

    public void setting(InputStream in) throws IOException {
        configHelper.setting(in);
    }

    public void addSetting(String key, String value) {
        configHelper.setting(key, value);
    }

    public void addSetting(String key, Boolean value) {
        configHelper.setting(key, value);
    }

    public void addSetting(String key, Integer value) {
        configHelper.setting(key, value);
    }

    public void setSettings(Settings settings) {
        configHelper.settings(settings);
    }

    public Settings getSettings() {
        return configHelper.settings();
    }

    public void mapping(String type, String mapping) throws IOException {
        configHelper.mapping(type, mapping);
    }

    public void mapping(String type, InputStream in) throws IOException {
        configHelper.mapping(type, in);
    }

    public Map<String, String> getMappings() {
        return configHelper.mappings();
    }

}
