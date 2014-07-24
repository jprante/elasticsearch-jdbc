package org.xbib.elasticsearch.plugin.feeder;

import java.io.Reader;

public interface CommandLineInterpreter extends Runnable {

    CommandLineInterpreter readFrom(Reader reader);

    void schedule(Thread thread);

    void shutdown();

    Thread shutdownHook();

}
