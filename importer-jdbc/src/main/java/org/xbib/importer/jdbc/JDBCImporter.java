package org.xbib.importer.jdbc;

import org.xbib.content.settings.Settings;
import org.xbib.importer.Importer;
import org.xbib.importer.Program;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 */
public class JDBCImporter implements Program {

    private static final Logger logger = Logger.getLogger(JDBCImporter.class.getName());

    @Override
    public int run(Settings settings) throws IOException {
        try (final Importer importer = new Importer(settings, "jdbc")) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    importer.close();
                } catch (Exception e) {
                    logger.log(Level.SEVERE, e.getMessage(), e);
                }
            }));
            importer.open();
        } catch (Exception e) {
            logger.log(Level.SEVERE, e.getMessage(), e);
            return 1;
        }
        return 0;
    }
}
