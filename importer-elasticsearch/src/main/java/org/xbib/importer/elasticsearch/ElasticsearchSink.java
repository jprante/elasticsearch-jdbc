package org.xbib.importer.elasticsearch;

import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.VersionType;
import org.xbib.content.settings.Settings;
import org.xbib.elasticsearch.extras.client.ClientMethods;
import org.xbib.importer.Document;
import org.xbib.importer.ImporterListener;
import org.xbib.importer.Sink;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 */
public class ElasticsearchSink implements Sink {

    private final static Logger logger = Logger.getLogger(ElasticsearchSink.class.getName());

    private final Settings settings;

    private final ImporterListener listener;

    private ClientMethods client;

    private ElasticsearchMetrics elasticsearchMetrics;

    private ElasticsearchOutput elasticsearchOutput;

    private Map<String, ElasticsearchIndexDefinition> indexDefinitionMap;

    public ElasticsearchSink(Settings settings, ImporterListener listener) throws IOException {
        this.settings = settings;
        this.listener = listener;
        this.elasticsearchOutput = new ElasticsearchOutput();
        this.elasticsearchMetrics = new ElasticsearchMetrics();
        logger.log(Level.INFO, "settings=" + settings.getAsMap());
        this.client = elasticsearchOutput.createClient(settings);
        this.indexDefinitionMap = elasticsearchOutput.makeIndexDefinitions(client,
                settings.getGroups("index"));
        elasticsearchMetrics.scheduleBulkMetric(settings, client.getMetric());
        logger.log(Level.INFO, "creation of " + indexDefinitionMap.keySet());
        for (Map.Entry<String, ElasticsearchIndexDefinition> entry : indexDefinitionMap.entrySet()) {
            elasticsearchOutput.createIndex(client, entry.getValue());
        }
        logger.log(Level.INFO, "startup of " + indexDefinitionMap.keySet());
        elasticsearchOutput.startup(client, indexDefinitionMap);
    }

    @Override
    public void index(Document document, boolean create) throws IOException {
        if (client == null) {
            return;
        }
        String source = document.build();
        if (source.isEmpty() || "{}".equals(source)) {
            return;
        }
        ElasticsearchIndexDefinition indexDefinition = indexDefinitionMap.get("default");
        setCoordinates(indexDefinition, document);
        if (indexDefinition.getConcreteIndex() == null || indexDefinition.getType() == null) {
            return;
        }
        IndexRequest request = new IndexRequest().index(indexDefinition.getConcreteIndex())
                .type(indexDefinition.getType())
                .id(indexDefinition.getId())
                .source(source);
        if (document.hasMeta("_version")) {
            request.versionType(VersionType.EXTERNAL)
                    .version(Long.parseLong(document.getMeta("_version")));
        }
        if (document.hasMeta("_routing")) {
            request.routing(document.getMeta("_routing"));
        }
        if (document.hasMeta("_parent")) {
            request.parent(document.getMeta("_parent"));
        }
        if (document.hasMeta("_timestamp")) {
            request.timestamp(document.getMeta("_timestamp"));
        }
        if (document.hasMeta("_ttl")) {
            request.ttl(Long.parseLong(document.getMeta("_ttl")));
        }
        if (logger.isLoggable(Level.FINER)) {
            logger.log(Level.FINER, "adding bulk index action " + request.source().utf8ToString());
        }
        client.bulkIndex(request);
    }

    @Override
    public void delete(Document document) {
        if (client == null) {
            return;
        }
        if (document.getIndex() == null || document.getType() == null || document.getId() == null) {
            return;
        }
        ElasticsearchIndexDefinition indexDefinition = indexDefinitionMap.get("default");
        setCoordinates(indexDefinition, document);
        if (indexDefinition.getConcreteIndex() == null ||
                indexDefinition.getType() == null ||
                indexDefinition.getId() == null) {
            return;
        }
        DeleteRequest request = new DeleteRequest()
                .index(indexDefinition.getConcreteIndex())
                .type(indexDefinition.getType())
                .id(indexDefinition.getId());
        if (document.hasMeta("_version")) {
            request.versionType(VersionType.EXTERNAL)
                    .version(Long.parseLong(document.getMeta("_version")));
        }
        if (document.hasMeta("_routing")) {
            request.routing(document.getMeta("_routing"));
        }
        if (document.hasMeta("_parent")) {
            request.parent(document.getMeta("_parent"));
        }
        if (logger.isLoggable(Level.FINER)) {
            logger.log(Level.FINE, "adding bulk delete action " + request.index() + "/" + request.type() + "/" + request.id());
        }
        client.bulkDelete(request);
    }

    @Override
    public void update(Document document) throws IOException {
        if (client == null) {
            return;
        }
        ElasticsearchIndexDefinition indexDefinition = indexDefinitionMap.get("default");
        setCoordinates(indexDefinition, document);
        if (indexDefinition.getConcreteIndex() == null ||
                indexDefinition.getType() == null ||
                indexDefinition.getId() == null) {
            return;
        }
        UpdateRequest request = new UpdateRequest()
                .index(indexDefinition.getConcreteIndex())
                .type(indexDefinition.getType())
                .id(indexDefinition.getId()).doc(document.getSource());
        request.docAsUpsert(true);

        if (document.hasMeta("_version")) {
            request.versionType(VersionType.EXTERNAL)
                    .version(Long.parseLong(document.getMeta("_version")));
        }
        if (document.hasMeta("_routing")) {
            request.routing(document.getMeta("_routing"));
        }
        if (document.hasMeta("_parent")) {
            request.parent(document.getMeta("_parent"));
        }
        if (logger.isLoggable(Level.FINER)) {
            logger.log(Level.FINER,
                    "adding bulk update action " + request.index() + "/" + request.type() + "/" + request.id());
        }
        client.bulkUpdate(request);
    }

    @Override
    public void flush() throws IOException {
        if (client == null) {
            return;
        }
        client.flushIngest();
        // wait for all outstanding bulk requests before continuing. Estimation is 60 seconds
        try {
            client.waitForResponses(TimeValue.timeValueSeconds(60));
        } catch (InterruptedException e) {
            logger.log(Level.WARNING, "interrupted while waiting for responses");
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            logger.log(Level.WARNING, "exception while executing", e);
        }
    }

    @Override
    public void close() {
        if (client == null) {
            return;
        }
        try {
            elasticsearchOutput.close(client, indexDefinitionMap);
            client.shutdown();
        } catch (IOException e) {
            logger.log(Level.SEVERE, e.getMessage(), e);
        }
    }

    private void setCoordinates(ElasticsearchIndexDefinition indexDefinition, Document object) {
        if (object.getIndex() != null && !object.getIndex().isEmpty()) {
            indexDefinition.setIndex(object.getIndex());
        }
        if (object.getType() != null && !object.getType().isEmpty()) {
            indexDefinition.setType(object.getType());
        }
        if (object.getId() != null && !object.getId().isEmpty()) {
            indexDefinition.setId(object.getId());
        }
    }
}
