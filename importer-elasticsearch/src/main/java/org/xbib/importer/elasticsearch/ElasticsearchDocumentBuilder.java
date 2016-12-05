package org.xbib.importer.elasticsearch;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.xbib.content.settings.Settings;
import org.xbib.importer.Document;
import org.xbib.importer.Sink;
import org.xbib.importer.TabularDataStream;
import org.xbib.importer.plugin.DocumentPlugin;

import java.io.Flushable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 */
public class ElasticsearchDocumentBuilder
        implements DocumentPlugin, TabularDataStream<String, Object>, Flushable {

    private static final Logger logger = Logger.getLogger(ElasticsearchDocumentBuilder.class.getName());

    private final Settings settings;

    private final Sink output;

    /**
     * The current document.
     */
    private Document current;
    /**
     * The document which was built before the current document.
     */
    private Document prev;

    /**
     * The keys of the values. They are examined for the Elasticsearch index attributes.
     */
    private List<String> keys;

    private boolean shouldAutoGenID;

    private boolean shouldIgnoreNull = false;

    private final List<DocumentPlugin> plugins;

    public ElasticsearchDocumentBuilder(Sink output) {
        this(Settings.EMPTY_SETTINGS, output);
    }

    public ElasticsearchDocumentBuilder(Settings settings, Sink output) {
        this.settings = settings;
        this.output = output;
        this.plugins = new ArrayList<>();
        this.plugins.add(this);
        installPlugins(settings);
    }

    public ElasticsearchDocumentBuilder shouldIgnoreNull(boolean shouldIgnoreNull) {
        this.shouldIgnoreNull = shouldIgnoreNull;
        return this;
    }

    public ElasticsearchDocumentBuilder addPlugin(DocumentPlugin plugin) {
        plugins.add(0, plugin);
        return this;
    }

    @Override
    public void begin() throws IOException {
        // do nothing
    }

    @Override
    public void keys(List<String> keys) throws IOException {
        this.keys = keys;
        this.shouldAutoGenID = !keys.contains("_id");
    }

    @Override
    public void values(List<Object> values) throws IOException {
        boolean hasSource = false;
        if (current == null) {
            current = newDocument();
        }
        if (prev == null) {
            prev = newDocument();
        }
        // first pass, execute meta only
        for (int i = 0; i < keys.size() && i < values.size(); i++) {
            String k = keys.get(i);
            if (k.startsWith("_")) {
                Object v = values.get(i);
                for (DocumentPlugin dp : plugins) {
                    if (dp.executeMeta(current, k, v)) {
                        break;
                    }
                }
                if ("_source".equalsIgnoreCase(k)) {
                    hasSource = true;
                }
            }
        }
        // special case, _source in meta
        if (hasSource) {
            end(current);
            current = newDocument();
            return;
        }
        // coordinate check: switch to next document if current is not equal to previous
        if (!current.equals(prev) || shouldAutoGenID) {
            prev.setSource(current.getSource()); // "steal" source
            end(prev); // here, the element is being prepared for bulk indexing
            prev = current;
            current = newDocument();
        }
        // second pass, execute data only
        for (int i = 0; i < keys.size() && i < values.size(); i++) {
            String k = keys.get(i);
            if (!k.startsWith("_")) {
                Object v = values.get(i);
                for (DocumentPlugin dp : plugins) {
                    if (dp.execute(current, k, v)) {
                        break;
                    }
                }
            }
        }
        // third pass, add special meta data values
        String s = prev.getMeta("_timestamp");
        if (s != null ) {
            // timestamp only non-empty docs
            current.merge("timestamp", s);
        }
    }

    @Override
    public void flush() throws IOException {
        if (prev != null) {
            prev.setSource(current.getSource());
            end(prev);
        }
        prev = newDocument();
        current = newDocument();
    }

    @Override
    public boolean executeMeta(Document document, String k, Object v) throws IOException {
        switch (k.toLowerCase()) {
            case "_optype":
                document.setOperationType(v.toString());
                break;
            case "_index":
                document.setIndex(v.toString());
                break;
            case "_type":
                document.setType(v.toString());
                break;
            case "_id":
                document.setId(v.toString());
                break;
            case "_version":
                document.setMeta("_version", v.toString());
                break;
            case "_routing":
                document.setMeta("routing", v.toString());
                break;
            case "_parent":
                document.setMeta("_parent", v.toString());
                break;
            case "_ttl":
                document.setMeta("_ttl", v.toString());
                break;
            case "_timestamp":
                document.setMeta("_timestamp", v.toString());
                break;
            case "_count":
                document.setMeta("_count", v.toString());
                break;
            case "_source":
                document.setSource(JsonXContent.jsonXContent.createParser(v.toString()).map());
                break;
            default:
                return false;
        }
        return true;
    }

    @Override
    public boolean execute(Document document, String key, Object value) throws IOException {
        if (value == null) {
            return false;
        }
        document.merge(key, value);
        return true;
    }

    /**
     * End of values.
     *
     * @throws IOException if this method fails
     */
    public void end() throws IOException {
        flush();
    }

    /**
     * The document is complete. Push it to the sink.
     *
     * @param document the object
     * @return this value listener
     * @throws IOException when ending the object gives an error
     */
    public ElasticsearchDocumentBuilder end(Document document) throws IOException {
        if (output != null) {
            if ((document.getOperationType() == null || "index".equals(document.getOperationType())) &&
                    !document.getSource().isEmpty()) {
                output.index(document, false);
            } else if ("create".equals(document.getOperationType()) && !document.getSource().isEmpty()) {
                output.index(document, true);
            } else if ("update".equals(document.getOperationType()) && !document.getSource().isEmpty()) {
                output.update(document);
            } else if ("delete".equals(document.getOperationType())) {
                output.delete(document);
            }
        }
        return this;
    }

    /**
     * Create a new document.
     *
     * @return a new document
     */
    private Document newDocument() {
        Map<String,String> map = new HashMap<>();
        map.put("ignore_null", Boolean.toString(shouldIgnoreNull));
        map.put("force_array", "false");
        return new ElasticsearchDocument(new ToXContent.MapParams(map));
    }

    private void installPlugins(Settings settings) {
        Map<String, Settings> map = settings.getGroups("plugins");
        if (map == null) {
            return;
        }
        for (Map.Entry<String, Settings> entry : map.entrySet()) {
            String pluginClassName = entry.getValue().get("class");
            try {
                Class<?> clazz = Class.forName(pluginClassName);
                plugins.add((DocumentPlugin)clazz.newInstance());
            } catch (ClassNotFoundException  | IllegalAccessException  | InstantiationException e) {
                logger.log(Level.SEVERE, e.getMessage(), e);
            }
        }
    }
}
