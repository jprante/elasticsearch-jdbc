package org.xbib.importer;

import org.xbib.content.settings.Settings;

/**
 */
public interface ImporterListener {

    void connected(Worker<Settings> worker);

    void received(Worker<Settings> worker);

    void disconnected(Worker<Settings> worker);

    void exception(Worker<Settings> worker, Throwable throwable);
}
