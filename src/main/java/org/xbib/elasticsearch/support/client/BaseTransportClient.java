package org.xbib.elasticsearch.support.client;

import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.URI;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.collect.Sets.newHashSet;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;

public abstract class BaseTransportClient {

    private final static ESLogger logger = ESLoggerFactory.getLogger(BaseTransportClient.class.getSimpleName());

    private final static String DEFAULT_CLUSTER_NAME = "elasticsearch";

    private final Set<InetSocketTransportAddress> addresses = newHashSet();

    protected TimeValue maxWaitTime = new TimeValue(60, TimeUnit.SECONDS);

    protected TransportClient client;

    protected ConfigHelper configHelper = new ConfigHelper();

    public BaseTransportClient newClient(URI uri, Settings settings) {
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
        } else {
            logger.info("creating transport client, java version {}, using default settings",
                    System.getProperty("java.version"));
            this.client = new TransportClient();
        }
        try {
            connect(uri, settings);
        } catch (UnknownHostException e) {
            logger.error(e.getMessage(), e);
        } catch (SocketException e) {
            logger.error(e.getMessage(), e);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
        return this;
    }

    public Settings defaultSettings(URI uri) {
        return settingsBuilder()
                .put("cluster.name", findClusterName(uri))
                .put("network.server", false)
                .put("node.client", true)
                .put("client.transport.sniff", false)
                .put("client.transport.ignore_cluster_name", false)
                .put("client.transport.ping_timeout", "30s")
                .put("client.transport.nodes_sampler_interval", "30s")
                .put("action.ingest.timeout", maxWaitTime)
                .build();
    }

    public Client client() {
        return client;
    }

    public String healthColor() {
        return ClientHelper.healthColor(client);
    }

    public List<String> getConnectedNodes() {
        return ClientHelper.getConnectedNodes(client);
    }

    public synchronized void shutdown() {
        if (client != null) {
            logger.debug("shutdown started");
            client.close();
            client.threadPool().shutdown();
            client = null;
            logger.debug("shutdown complete");
        }
        addresses.clear();
    }

    protected URI findURI() {
        URI uri = null;
        String hostname = "localhost";
        try {
            hostname = InetAddress.getLocalHost().getHostName();
            logger.debug("the hostname is {}", hostname);
            uri = URI.create("es://" + hostname + ":9300");
            // custom?
            URL url = getClass().getResource("/org/xbib/elasticsearch/cluster.properties");
            if (url != null) {
                InputStream in = url.openStream();
                Properties p = new Properties();
                p.load(in);
                in.close();
                // the properties contains default URIs per hostname
                if (p.containsKey(hostname)) {
                    uri = URI.create(p.getProperty(hostname));
                    logger.debug("custom URI found in cluster.properties for hostname {}: {}", hostname, uri);
                    return uri;
                }
            }
        } catch (UnknownHostException e) {
            logger.warn("can't resolve host name, probably something wrong with network config: " + e.getMessage(), e);
        } catch (Exception e) {
            logger.warn(e.getMessage(), e);
        }
        logger.debug("URI for hostname {}: {}", hostname, uri);
        return uri;
    }

    protected String findClusterName(URI uri) {
        String clustername;
        try {
            Map<String, String> params = parseQueryString(uri, "UTF-8");
            clustername = params.get("es.cluster.name");
            if (clustername != null) {
                logger.info("cluster name found in URI {}: {}", uri, clustername);
                return clustername;
            }
            clustername = params.get("cluster.name");
            if (clustername != null) {
                logger.info("cluster name found in URI {}: {}", uri, clustername);
                return clustername;
            }
        } catch (UnsupportedEncodingException ex) {
            logger.warn(ex.getMessage(), ex);
        }
        logger.info("cluster name not found in URI {}, parameter es.cluster.name", uri);
        clustername = System.getProperty("es.cluster.name");
        if (clustername != null) {
            logger.info("cluster name found in es.cluster.name system property: {}", clustername);
            return clustername;
        }
        clustername = System.getProperty("cluster.name");
        if (clustername != null) {
            logger.info("cluster name found in cluster.name system property: {}", clustername);
            return clustername;
        }
        logger.info("cluster name not found, falling back to default: {}", DEFAULT_CLUSTER_NAME);
        clustername = DEFAULT_CLUSTER_NAME;
        return clustername;
    }

    protected void connect(URI uri, Settings settings) throws IOException {
        String hostname = uri.getHost();
        int port = uri.getPort();
        boolean newaddresses = false;
        if (!"es".equals(uri.getScheme())) {
            logger.warn("please specify URI scheme 'es'");
        }
        if ("hostname".equals(hostname)) {
            InetSocketTransportAddress address = new InetSocketTransportAddress(InetAddress.getLocalHost().getHostName(), port);
            if (!addresses.contains(address)) {
                logger.info("adding hostname address for transport client: {}", address);
                client.addTransportAddress(address);
                addresses.add(address);
                newaddresses = true;
            }
        } else if ("interfaces".equals(hostname)) {
            Enumeration<NetworkInterface> nets = NetworkInterface.getNetworkInterfaces();
            for (NetworkInterface netint : Collections.list(nets)) {
                logger.info("checking network interface = {}", netint.getName());
                Enumeration<InetAddress> inetAddresses = netint.getInetAddresses();
                for (InetAddress addr : Collections.list(inetAddresses)) {
                    logger.info("checking address = {}", addr.getHostAddress());
                    InetSocketTransportAddress address = new InetSocketTransportAddress(addr, port);
                    if (!addresses.contains(address)) {
                        logger.info("adding address to transport client: {}", address);
                        client.addTransportAddress(address);
                        addresses.add(address);
                        newaddresses = true;
                    }
                }
            }
        } else if ("inet4".equals(hostname)) {
            Enumeration<NetworkInterface> nets = NetworkInterface.getNetworkInterfaces();
            for (NetworkInterface netint : Collections.list(nets)) {
                logger.info("checking network interface = {}", netint.getName());
                Enumeration<InetAddress> inetAddresses = netint.getInetAddresses();
                for (InetAddress addr : Collections.list(inetAddresses)) {
                    if (addr instanceof Inet4Address) {
                        logger.info("checking address = {}", addr.getHostAddress());
                        InetSocketTransportAddress address = new InetSocketTransportAddress(addr, port);
                        if (!addresses.contains(address)) {
                            logger.info("adding address for transport client: {}", address);
                            client.addTransportAddress(address);
                            addresses.add(address);
                            newaddresses = true;
                        }
                    }
                }
            }
        } else if ("inet6".equals(hostname)) {
            Enumeration<NetworkInterface> nets = NetworkInterface.getNetworkInterfaces();
            for (NetworkInterface netint : Collections.list(nets)) {
                logger.info("checking network interface = {}", netint.getName());
                Enumeration<InetAddress> inetAddresses = netint.getInetAddresses();
                for (InetAddress addr : Collections.list(inetAddresses)) {
                    if (addr instanceof Inet6Address) {
                        logger.info("checking address = {}", addr.getHostAddress());
                        InetSocketTransportAddress address = new InetSocketTransportAddress(addr, port);
                        if (!addresses.contains(address)) {
                            logger.info("adding address for transport client: {}", address);
                            client.addTransportAddress(address);
                            addresses.add(address);
                            newaddresses = true;
                        }
                    }
                }
            }
        } else {
            InetSocketTransportAddress address = new InetSocketTransportAddress(hostname, port);
            if (!addresses.contains(address)) {
                logger.info("adding custom address for transport client: {}", address);
                client.addTransportAddress(address);
                addresses.add(address);
                newaddresses = true;
            }
        }
        logger.info("configured addresses to connect: {}", addresses);
        if (client.connectedNodes() != null) {
            List<DiscoveryNode> nodes = client.connectedNodes().asList();
            logger.info("connected nodes = {}", nodes);
            if (newaddresses) {
                for (DiscoveryNode node : nodes) {
                    logger.info("new connection to {}", node);
                }
                if (!nodes.isEmpty()) {
                    Map<String, String> params = parseQueryString(uri, "UTF-8");
                    if (params.containsKey("es.sniff")) {
                        try {
                            connectMore();
                        } catch (Exception e) {
                            logger.error("error while connecting to more nodes", e);
                        }
                    }
                }
            }
        }
    }

    public ImmutableSettings.Builder getSettingsBuilder() {
        return configHelper.settingsBuilder();
    }

    public void resetSettings() {
        configHelper.reset();
    }

    public void addSetting(InputStream in) throws IOException {
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

    public void addMapping(String type, InputStream in) throws IOException {
        configHelper.mapping(type, in);
    }

    public void addMapping(String type, String mapping) {
        configHelper.mapping(type, mapping);
    }

    public String defaultMapping() throws IOException {
        return configHelper.defaultMapping();
    }

    public Map<String, String> getMappings() {
        return configHelper.mappings();
    }

    private void connectMore() throws IOException {
        logger.info("trying to discover more nodes...");
        ClusterStateResponse clusterStateResponse = client.admin().cluster().state(new ClusterStateRequest()).actionGet();
        DiscoveryNodes nodes = clusterStateResponse.getState().getNodes();
        for (DiscoveryNode node : nodes) {
            logger.info("adding discovered node {}", node);
            try {
                client.addTransportAddress(node.address());
            } catch (Exception e) {
                logger.warn("can't add node " + node, e);
            }
        }
        logger.info("... discovery done");
    }

    private Map<String, String> parseQueryString(URI uri, String encoding)
            throws UnsupportedEncodingException {
        Map<String, String> m = new HashMap<String, String>();
        if (uri == null) {
            throw new IllegalArgumentException();
        }
        if (uri.getRawQuery() == null) {
            return m;
        }
        // getRawQuery() because we do our decoding by ourselves
        StringTokenizer st = new StringTokenizer(uri.getRawQuery(), "&");
        while (st.hasMoreTokens()) {
            String pair = st.nextToken();
            int pos = pair.indexOf('=');
            if (pos < 0) {
                m.put(pair, null);
            } else {
                m.put(pair.substring(0, pos), decode(pair.substring(pos + 1, pair.length()), encoding));
            }
        }
        return m;
    }

    private String decode(String s, String encoding) {
        StringBuilder sb = new StringBuilder();
        boolean fragment = false;
        for (int i = 0; i < s.length(); i++) {
            char ch = s.charAt(i);
            switch (ch) {
                case '+':
                    sb.append(' ');
                    break;
                case '#':
                    sb.append(ch);
                    fragment = true;
                    break;
                case '%':
                    if (!fragment) {
                        // fast hex decode
                        sb.append((char) ((Character.digit(s.charAt(++i), 16) << 4)
                                | Character.digit(s.charAt(++i), 16)));
                    } else {
                        sb.append(ch);
                    }
                    break;
                default:
                    sb.append(ch);
                    break;
            }
        }
        try {
            // URL default encoding is ISO-8859-1
            return new String(sb.toString().getBytes("ISO-8859-1"), encoding);
        } catch (UnsupportedEncodingException e) {
            throw new Error("encoding " + encoding + " not supported");
        }
    }

}
