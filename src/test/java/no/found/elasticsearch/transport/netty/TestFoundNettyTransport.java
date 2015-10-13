
package no.found.elasticsearch.transport.netty;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterNameModule;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.ChannelPipeline;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.indices.breaker.CircuitBreakerModule;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPoolModule;
import org.elasticsearch.transport.TransportModule;
import org.elasticsearch.transport.netty.FoundNettyTransport;
import org.testng.annotations.Test;

import java.lang.reflect.Field;

import static org.testng.Assert.assertNotNull;

public class TestFoundNettyTransport {

    @Test
    public void testClientBootstrapUpdated() throws Exception {
        Settings settings = Settings.settingsBuilder()
                .put("name", "found-client-test")
                .put("transport.type", "org.elasticsearch.transport.netty.FoundNettyTransport")
                .put("path.home", System.getProperty("path.home"))
                .put("plugin.types", FoundTransportPlugin.class)
            .build();

        TransportModule transportModule = new TransportModule(settings);
        transportModule.addTransport("org.elasticsearch.transport.netty.FoundNettyTransport", FoundNettyTransport.class);

        ModulesBuilder modules = new ModulesBuilder();
        modules.add(new Version.Module(Version.CURRENT));
        modules.add(new SettingsModule(settings));
        modules.add(new ClusterNameModule(settings));
        modules.add(transportModule);
        modules.add(new ThreadPoolModule(new ThreadPool(settings)));
        modules.add(new CircuitBreakerModule(settings));

        Injector injector = modules.createInjector();

        FoundNettyTransport transport = injector.getInstance(FoundNettyTransport.class);
        transport.start();

        Field clientBootstrapField = transport.getClass().getSuperclass().getDeclaredField("clientBootstrap");
        clientBootstrapField.setAccessible(true);
        ClientBootstrap clientBootstrap = (ClientBootstrap)clientBootstrapField.get(transport);

        ChannelPipeline pipeline = clientBootstrap.getPipelineFactory().getPipeline();

        FoundAuthenticatingChannelHandler channelHandler = pipeline.get(FoundAuthenticatingChannelHandler.class);
        assertNotNull(channelHandler);

        transport.stop();
        transport.close();

    }
}