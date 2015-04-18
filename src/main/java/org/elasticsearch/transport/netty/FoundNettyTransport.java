/*
 * This is the MIT license: http://www.opensource.org/licenses/mit-license.php
 *
 * Copyright (c) 2010-2012, Found IT A/S.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this
 * software and associated documentation files (the "Software"), to deal in the Software
 * without restriction, including without limitation the rights to use, copy, modify, merge,
 * publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons
 * to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or
 * substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR
 * PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE
 * FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
 * OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 */
package org.elasticsearch.transport.netty;

import no.found.elasticsearch.transport.netty.FoundAuthenticatingChannelHandler;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.netty.bootstrap.ClientBootstrap;
import org.elasticsearch.common.netty.channel.ChannelHandlerContext;
import org.elasticsearch.common.netty.channel.ChannelPipeline;
import org.elasticsearch.common.netty.channel.ChannelPipelineFactory;
import org.elasticsearch.common.netty.channel.ExceptionEvent;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.threadpool.ThreadPool;

import javax.net.ssl.SSLException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A transport that works with Found Elasticsearch.
 * <p>New settings introduced by this transport:</p>
 * <ul>
 * <li>{@code transport.found.host-suffixes}: A comma-separated list of host suffixes that
 * trigger our attempt to authenticate with Found Elasticsearch. Defaults to
 * {@code foundcluster.com, found.no}".</li>
 * <li>{@code transport.found.ssl-ports}: A comma-separated list of ports that trigger our
 * SSL support. Defaults to {@code 9343}".</li>
 * <li>{@code transport.found.api-key}: An API-key which is used to authorize this client
 * when connecting to Found Elasticsearch. API-keys are managed via the console as
 * a list of Strings under the root level key "api_keys". Defaults to
 * {@code missing-api-key}</li>
 * <li>{@code transport.found.connection-keep-alive-interval}: The interval in which to send
 * keep-alive messages. Defaults to {@code 20s}. Set to 0 to disable.</li>
 * <li>{@code transport.found.ssl.unsafe_allow_self_signed}: Whether to accept self-signed
 * certificates when using SSL. This is unsafe and allows for MITM-attacks, but
 * may be useful for testing. Defaults to {@code false}.</li>
 * </ul>
 * <p><b>The transport is backwards-compatible with the default transport.</b></p>
 * <p>Example configuration:</p>
 * <pre>
 * {@code // Build the settings for our client.
 *   Settings settings = ImmutableSettings.settingsBuilder()
 *       // Setting "transport.type" enables this transport (1.4+):
 *       .put("transport.type", "org.elasticsearch.transport.netty.FoundNettyTransport")
 *       // Create an api key via the console and add it here:
 *       .put("transport.found.api-key", "YOUR_API_KEY")
 *
 *       .put("cluster.name", "YOUR_CLUSTER_ID")
 *
 *       .put("client.transport.ignore_cluster_name", false)
 *
 *       .build();
 *
 *   // Instantiate a TransportClient and add Found Elasticsearch to the list of addresses to connect to.
 *   // Only port 9343 (SSL-encrypted) is currently supported.
 *   Client client = new TransportClient(settings)
 *       .addTransportAddress(new InetSocketTransportAddress("YOUR_CLUSTER_ID-REGION.foundcluster.com", 9343));
 * }
 * </pre>
 * <p>Example usage:</p>
 * <pre>
 * {@code
 *  System.out.print("Getting cluster health... "); System.out.flush();
 *  ActionFuture<ClusterHealthResponse> healthFuture = client.admin().cluster().health(Requests.clusterHealthRequest());
 *  ClusterHealthResponse healthResponse = healthFuture.get(5, TimeUnit.SECONDS);
 *  System.out.println("Got response: " + healthResponse.getStatus());
 * }
 * </pre>
 */
public class FoundNettyTransport extends NettyTransport {
    private final String[] hostSuffixes;
    private final int[] sslPorts;
    private final String apiKey;
    private final boolean unsafeAllowSelfSigned;
    private final TimeValue keepAliveInterval;
    private final ClusterName clusterName;
    private final ScheduledExecutorService scheduler;

    @Inject
    public FoundNettyTransport(Settings settings, ThreadPool threadPool, NetworkService networkService, ClusterName clusterName, BigArrays bigArrays, Version version) {
        super(settings, threadPool, networkService, bigArrays, version);

        if (settings.getAsBoolean("client.transport.ignore_cluster_name", false)) {
            logger.warn("client.transport.ignore_cluster_name has been set to true! " +
                    "This is not recommended in combination with Found Elasticsearch Transport module.");
        }

        if (settings.getAsBoolean("client.transport.sniff", false)) {
            throw new ElasticsearchException("The transport client setting \"client.transport.sniff\" is [true], which is not supported by this transport.");
        }

        this.scheduler = threadPool.scheduler();
        this.clusterName = clusterName;

        keepAliveInterval = settings.getAsTime("transport.found.connection-keep-alive-interval", new TimeValue(20000, TimeUnit.MILLISECONDS));
        unsafeAllowSelfSigned = settings.getAsBoolean("transport.found.ssl.unsafe_allow_self_signed", false);
        hostSuffixes = settings.getAsArray("transport.found.host-suffixes", new String[]{".foundcluster.com", ".found.no"});

        List<Integer> ports = new LinkedList<Integer>();
        for (String strPort : settings.getAsArray("transport.found.ssl-ports", new String[]{"9343"})) {
            try {
                ports.add(Integer.parseInt(strPort));
            } catch (NumberFormatException nfe) {
                // ignore
            }
        }
        sslPorts = new int[ports.size()];
        for (int i = 0; i < ports.size(); i++) {
            sslPorts[i] = ports.get(i);
        }

        this.apiKey = settings.get("transport.found.api-key", "missing-api-key");
    }

    @Override
    void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        if (e.getCause() instanceof SSLException) {
            return;
        }
        super.exceptionCaught(ctx, e);
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        super.doStart();

        try {
            Field clientBootstrapField = getClass().getSuperclass().getDeclaredField("clientBootstrap");
            clientBootstrapField.setAccessible(true);
            ClientBootstrap clientBootstrap = (ClientBootstrap) clientBootstrapField.get(this);

            final ChannelPipelineFactory originalFactory = clientBootstrap.getPipelineFactory();

            clientBootstrap.setPipelineFactory(new ChannelPipelineFactory() {
                @Override
                public ChannelPipeline getPipeline() throws Exception {
                    ChannelPipeline pipeline = originalFactory.getPipeline();
                    pipeline.addFirst("found-authenticating-channel-handler", new FoundAuthenticatingChannelHandler(logger, scheduler, clusterName, keepAliveInterval, unsafeAllowSelfSigned, hostSuffixes, sslPorts, apiKey));
                    return pipeline;
                }
            });

            clientBootstrapField.setAccessible(false);
        } catch (ReflectiveOperationException roe) {
            logger.error("Unable to update the transport pipeline. Plugin upgrade required.", roe);
        }
    }

    @Override
    public void connectToNode(DiscoveryNode node, boolean light) {
        // we hook into the connection here and use reflection in order to update the
        // resolved address of the given node by resolving it again. the rationale behind
        // this is that the ELB addresses may change and that Elasticsearch otherwise doesn't
        // try to resolve it again.

        if (node.address() instanceof InetSocketTransportAddress) {
            InetSocketTransportAddress oldAddress = (InetSocketTransportAddress) node.address();
            InetSocketTransportAddress newAddress = new InetSocketTransportAddress(oldAddress.address().getHostString(), oldAddress.address().getPort());

            boolean oldResolved = !oldAddress.address().isUnresolved();
            boolean newResolved = !newAddress.address().isUnresolved();

            boolean resolvedOk = !oldResolved || newResolved;

            // only update it if the old one was not resolved, or the new address is resolved AND the address has changed.
            if (resolvedOk && !Arrays.equals(oldAddress.address().getAddress().getAddress(), newAddress.address().getAddress().getAddress())) {
                try {
                    Field addressField = node.getClass().getDeclaredField("address");

                    boolean wasAccessible = addressField.isAccessible();
                    addressField.setAccessible(true);

                    addressField.set(node, newAddress);

                    addressField.setAccessible(wasAccessible);

                    logger.info("Updated the resolved address of [{}] from [{}] to [{}]", node, oldAddress, newAddress);
                } catch (ReflectiveOperationException roe) {
                    logger.error("Unable to update the resolved address of [{}]. Plugin upgrade likely required.", roe, node);
                }
            }
        }
        super.connectToNode(node, light);
    }
}
