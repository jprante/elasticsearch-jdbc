package org.xbib.importer;

import org.xbib.content.settings.Settings;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 */
public class DefaultImporterListener implements ImporterListener {

    private static final Logger logger = Logger.getLogger(DefaultImporterListener.class.getName());

    @Override
    public void connected(Worker<Settings> worker) {
        logger.log(Level.INFO, "connected: " + worker);
    }

    @Override
    public void received(Worker<Settings> worker) {
        logger.log(Level.INFO, "received: " + worker);
    }

    @Override
    public void disconnected(Worker<Settings> worker) {
        logger.log(Level.INFO,"disconnected: " + worker);
    }

    @Override
    public void exception(Worker<Settings> worker, Throwable throwable) {
        logger.log(Level.SEVERE, worker + ": " + throwable.getMessage(), throwable);
    }
}
