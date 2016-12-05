package org.xbib.importer.jdbc;

import org.xbib.content.XContentBuilder;
import org.xbib.content.json.JsonXContent;
import org.xbib.content.settings.PlaceholderResolver;
import org.xbib.content.settings.PropertyPlaceholder;
import org.xbib.content.settings.Settings;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 */
class JDBCState {

    private static final Logger logger = Logger.getLogger(JDBCState.class.getName());

    private final Settings settings;

    private final Settings request;

    private final Map<String, Object> variables;

    JDBCState(Settings settings, Settings request, Map<String, Object> variables) {
        this.settings = settings;
        this.request = request;
        this.variables = variables;
    }

    void setVariable(String key, Object value) {
        variables.put(key, value);
    }

    @SuppressWarnings("unchecked")
    <T> T getVariable(String key) {
        return (T) variables.get(key);
    }

    public void put(Map<String, Object> map) {
        variables.putAll(map);
    }

    Object interpolate(Object value) {
        if (value instanceof String) {
            String s = (String) value;
            if ("$now".equals(s)) {
                return new Timestamp(Instant.now().toEpochMilli());
            } else {
                return variables.containsKey(s) ? variables.get(s) : s;
            }
        }
        return value;
    }

    void save() throws IOException {
        String statefile = settings.get("statefile");
        if (statefile == null) {
            return;
        }
        File file = new File(statefile);
        if (file.getParentFile() != null && !file.getParentFile().exists()) {
            file.getParentFile().mkdirs();
        }
        if (!file.exists() || file.canWrite()) {
            Writer writer = new FileWriter(statefile);
            //FormatDateTimeFormatter formatter = Joda.forPattern("dateOptionalTime");
            PropertyPlaceholder propertyPlaceholder =
                    new PropertyPlaceholder("${", "}", false);
            Settings.Builder settingsBuilder = Settings.settingsBuilder()
                    .put(settings)
                    .replacePropertyPlaceholders(propertyPlaceholder, new StatePlaceHolderResolver());
            //.put("metrics.lastexecutionstart", formatter.printer().print(source.getMetric().getLastExecutionStart()))
            //.put("metrics.lastexecutionend", formatter.printer().print(source.getMetric().getLastExecutionEnd()))
            //.put("metrics.counter", source.getMetric().getCounter());
            XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint()
                    .startObject()
                    .field("type", "jdbc")
                    .field("jdbc")
                    .map(settingsBuilder.build().getAsStructuredMap())
                    .endObject();
            writer.write(builder.string());
            writer.close();
            if (file.length() > 0) {
                logger.log(Level.INFO, "state persisted to " + statefile);
            } else {
                logger.log(Level.SEVERE, "state file truncated!");
            }
        } else {
            logger.log(Level.WARNING,"can't write to {}", statefile);
        }
    }

    void load() throws IOException {
        String statefile = settings.get("jdbc.statefile");
        if (statefile != null) {
            try {
                File file = new File(statefile);
                if (file.exists() && file.isFile() && file.canRead()) {
                    InputStream stateFileInputStream = new FileInputStream(file);
                    Settings.settingsBuilder()
                            .put(settings)
                            .loadFromStream("statefile", stateFileInputStream)
                            .build();
                    // TODO fill variables from settings
                } else {
                    logger.log(Level.SEVERE, "can't read from {}, skipped", statefile);
                }
            } catch (IOException e) {
                logger.log(Level.SEVERE, e.getMessage(), e);
            }
        }
    }

    class StatePlaceHolderResolver implements PlaceholderResolver {

        @Override
        public String resolvePlaceholder(String placeholderName) {
            return null;
        }
    }
}
