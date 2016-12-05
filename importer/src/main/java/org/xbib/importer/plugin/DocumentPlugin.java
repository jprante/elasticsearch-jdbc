package org.xbib.importer.plugin;

import org.xbib.importer.Document;

import java.io.IOException;

/**
 */
public interface DocumentPlugin {

    boolean execute(Document document, String key, Object value) throws IOException;

    boolean executeMeta(Document document, String key, Object value) throws IOException;
}
