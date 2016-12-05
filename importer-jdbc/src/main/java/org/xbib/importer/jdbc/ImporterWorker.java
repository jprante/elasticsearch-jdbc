package org.xbib.importer.jdbc;

import org.xbib.content.settings.Settings;
import org.xbib.importer.ImporterListener;
import org.xbib.importer.Sink;
import org.xbib.importer.Worker;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 */
public class ImporterWorker implements Worker<Settings> {

    private final Logger logger = Logger.getLogger(ImporterWorker.class.getName());

    private final Settings settings;

    private final Sink sink;

    private final ImporterListener listener;

    private final Map<String, Object> variables;

    private JDBCState jdbcState;

    private Settings request;

    public ImporterWorker(Settings settings, Sink sink, ImporterListener listener) {
        this.settings = settings;
        this.sink = sink;
        this.listener = listener;
        this.variables = new HashMap<>();
        logger.log(Level.INFO, "importer worker: " + this + " settings=" + settings.getAsMap());
    }

    @Override
    public void execute(Settings request) throws IOException {
        logger.log(Level.INFO, "receiving request=" + request.getAsMap());
        this.request = request;
        this.jdbcState = new JDBCState(settings, request, variables);
        //jdbcState.setVariable("$metrics.count", 0L);
        try (JDBCSource source = new JDBCSource(settings, request, sink, jdbcState)) {
            logger.info("source=" + source + " sink=" + sink);
            //jdbcState.load();
            jdbcState.setVariable("$metrics.lastexecutionstart", new Timestamp(Instant.now().toEpochMilli()));
            listener.connected(this);
            source.execute(listener);
            jdbcState.setVariable("$metrics.lastexecutionend",  new Timestamp(Instant.now().toEpochMilli()));
            Long count = jdbcState.getVariable("$metrics.count");
            if (count == null) {
                count = 0L;
            }
            jdbcState.setVariable("$metrics.count", count + 1);
            listener.disconnected(this);
            //jdbcState.save();
        } catch (Exception e) {
            listener.exception(this, e);
            logger.log(Level.SEVERE, e.getMessage(), e);
        } finally {
            logger.log(Level.INFO, "processed request=" + request.getAsMap());
        }
    }

    @Override
    public Settings getRequest() {
        return request;
    }

    @Override
    public void close() throws IOException {
        // do nothing
    }

    public JDBCState getState() {
        return jdbcState;
    }
}
