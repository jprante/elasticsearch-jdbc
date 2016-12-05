package org.xbib.importer;

import org.xbib.content.settings.Settings;

import java.io.IOException;

/**
 */
public interface Program {

    int run(Settings settings) throws IOException;
}
