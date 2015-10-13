package no.found.elasticsearch.transport.netty;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.TransportModule;
import org.elasticsearch.transport.netty.FoundNettyTransport;

public class FoundTransportPlugin extends Plugin {
    @Override
    public String name() {
        return "found-transport";
    }

    @Override
    public String description() {
        return "Found transport plugin";
    }

    public void onModule(TransportModule module) {
        module.addTransport("org.elasticsearch.transport.netty.FoundNettyTransport", FoundNettyTransport.class);
    }
}
