/*
 * Copyright (C) 2015 Jörg Prante
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.xbib.elasticsearch.jdbc.strategy.standard;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesAction;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.xbib.elasticsearch.common.metrics.SinkMetric;
import org.xbib.elasticsearch.common.util.ControlKeys;
import org.xbib.elasticsearch.common.util.IndexableObject;
import org.xbib.elasticsearch.helper.client.ClientAPI;
import org.xbib.elasticsearch.helper.client.ClientBuilder;
import org.xbib.elasticsearch.jdbc.strategy.Sink;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Standard sink implementation. This implementation uses bulk processing,
 * index name housekeeping (with replica/refresh), and metrics. It understands
 * _version, _routing, _timestamp, _parent, and _ttl metadata.
 */
public class StandardSink<C extends StandardContext> implements Sink<C> {

    private final static Logger logger = LogManager.getLogger("importer.jdbc.sink.standard");

    protected C context;

    protected ClientAPI clientAPI;

    protected String index;

    protected String type;

    protected String id;

    private final static SinkMetric sinkMetric = new SinkMetric().start();

    @Override
    public String strategy() {
        return "standard";
    }

    @Override
    public StandardSink<C> newInstance() {
        return new StandardSink<>();
    }

    @Override
    public StandardSink<C> setContext(C context) {
        this.context = context;
        return this;
    }

    @Override
    public SinkMetric getMetric() {
        return sinkMetric;
    }

    @Override
    public synchronized void beforeFetch() throws IOException {
        Settings settings = context.getSettings();
        String index = settings.get("index", "jdbc");
        String type = settings.get("type", "jdbc");
        if (clientAPI == null) {
            clientAPI = createClient(settings);
            if (clientAPI.client() != null) {
                int pos = index.indexOf('\'');
                if (pos >= 0) {
                    SimpleDateFormat formatter = new SimpleDateFormat();
                    formatter.applyPattern(index);
                    index = formatter.format(new Date());
                }
                try {
                    index = resolveAlias(index);
                } catch (Exception e) {
                    logger.warn("can not resolve index {}", index);
                }
                setIndex(index);
                setType(type);
                try {
                    createIndex(settings, index, type);
                } catch (IndexAlreadyExistsException e) {
                    logger.warn(e.getMessage());
                }
            }
            clientAPI.waitForCluster("YELLOW", TimeValue.timeValueSeconds(30));
        }
    }

    @Override
    public synchronized void afterFetch() throws IOException {
        if (clientAPI == null) {
            return;
        }
        logger.debug("afterFetch: flush");
        flushIngest();
        logger.debug("afterFetch: stop bulk");
        clientAPI.stopBulk(index);
        logger.debug("afterFetch: refresh index");
        clientAPI.refreshIndex(index);
        logger.debug("afterFetch: before client shutdown");
        clientAPI.shutdown();
        clientAPI = null;
        logger.debug("afterFetch: after client shutdown");
    }

    @Override
    public synchronized void shutdown() {
        if (clientAPI == null) {
            return;
        }
        try {
            logger.info("shutdown in progress");
            flushIngest();
            clientAPI.stopBulk(index);
            clientAPI.shutdown();
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }

    @Override
    public StandardSink setIndex(String index) {
        this.index = index.contains("'") ? DateTimeFormat.forPattern(index).print(new DateTime()) : index;
        return this;
    }

    @Override
    public String getIndex() {
        return index;
    }

    @Override
    public StandardSink setType(String type) {
        this.type = type;
        return this;
    }

    @Override
    public String getType() {
        return type;
    }

    @Override
    public StandardSink setId(String id) {
        this.id = id;
        return this;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public void index(IndexableObject object, boolean create) throws IOException {
        if (clientAPI == null) {
            return;
        }
        if (Strings.hasLength(object.index())) {
            setIndex(object.index());
        }
        if (Strings.hasLength(object.type())) {
            setType(object.type());
        }
        if (Strings.hasLength(object.id())) {
            setId(object.id());
        }
        IndexRequest request = Requests.indexRequest(this.index)
                .type(this.type)
                .id(getId())
                .source(object.build());
        if (object.meta(ControlKeys._version.name()) != null) {
            request.versionType(VersionType.EXTERNAL)
                    .version(Long.parseLong(object.meta(ControlKeys._version.name())));
        }
        if (object.meta(ControlKeys._routing.name()) != null) {
            request.routing(object.meta(ControlKeys._routing.name()));
        }
        if (object.meta(ControlKeys._parent.name()) != null) {
            request.parent(object.meta(ControlKeys._parent.name()));
        }
        if (object.meta(ControlKeys._timestamp.name()) != null) {
            request.timestamp(object.meta(ControlKeys._timestamp.name()));
        }
        if (object.meta(ControlKeys._ttl.name()) != null) {
            request.ttl(Long.parseLong(object.meta(ControlKeys._ttl.name())));
        }
        if (logger.isTraceEnabled()) {
            logger.trace("adding bulk index action {}", request.source().toUtf8());
        }
        clientAPI.bulkIndex(request);
    }

    @Override
    public void delete(IndexableObject object) {
        if (clientAPI == null) {
            return;
        }
        if (Strings.hasLength(object.index())) {
            this.index = object.index();
        }
        if (Strings.hasLength(object.type())) {
            this.type = object.type();
        }
        if (Strings.hasLength(object.id())) {
            setId(object.id());
        }
        if (getId() == null) {
            return; // skip if no doc is specified to delete
        }
        DeleteRequest request = Requests.deleteRequest(this.index).type(this.type).id(getId());
        if (object.meta(ControlKeys._version.name()) != null) {
            request.versionType(VersionType.EXTERNAL)
                    .version(Long.parseLong(object.meta(ControlKeys._version.name())));
        }
        if (object.meta(ControlKeys._routing.name()) != null) {
            request.routing(object.meta(ControlKeys._routing.name()));
        }
        if (object.meta(ControlKeys._parent.name()) != null) {
            request.parent(object.meta(ControlKeys._parent.name()));
        }
        if (logger.isTraceEnabled()) {
            logger.trace("adding bulk delete action {}/{}/{}", request.index(), request.type(), request.id());
        }
        clientAPI.bulkDelete(request);
    }

    @Override
    public void update(IndexableObject object) throws IOException {
        if (clientAPI == null) {
            return;
        }
        if (Strings.hasLength(object.index())) {
            this.index = object.index();
        }
        if (Strings.hasLength(object.type())) {
            this.type = object.type();
        }
        if (Strings.hasLength(object.id())) {
            setId(object.id());
        }
        if (getId() == null) {
            return; // skip if no doc is specified to delete
        }
        UpdateRequest request = new UpdateRequest().index(this.index).type(this.type).id(getId()).doc(object.source());
        request.docAsUpsert(true);

        if (object.meta(ControlKeys._version.name()) != null) {
            request.versionType(VersionType.EXTERNAL)
                    .version(Long.parseLong(object.meta(ControlKeys._version.name())));
        }
        if (object.meta(ControlKeys._routing.name()) != null) {
            request.routing(object.meta(ControlKeys._routing.name()));
        }
        if (object.meta(ControlKeys._parent.name()) != null) {
            request.parent(object.meta(ControlKeys._parent.name()));
        }
        if (logger.isTraceEnabled()) {
            logger.trace("adding bulk update action {}/{}/{}", request.index(), request.type(), request.id());
        }
        clientAPI.bulkUpdate(request);
    }

    @Override
    public void flushIngest() throws IOException {
        if (clientAPI == null) {
            return;
        }
        clientAPI.flushIngest();
        // wait for all outstanding bulk requests before continuing. Estimation is 60 seconds
        try {
            clientAPI.waitForResponses(TimeValue.timeValueSeconds(60));
        } catch (InterruptedException e) {
            logger.warn("interrupted while waiting for responses");
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            logger.warn("exception while executing", e);
        }
    }

    private ClientAPI createClient(Settings settings) {
        Settings.Builder settingsBuilder = Settings.settingsBuilder()
                .put("cluster.name", settings.get("elasticsearch.cluster.name", settings.get("elasticsearch.cluster", "elasticsearch")))
                .putArray("host", settings.getAsArray("elasticsearch.host"))
                .put("port", settings.getAsInt("elasticsearch.port", 9300))
                .put("sniff", settings.getAsBoolean("elasticsearch.sniff", false))
                .put("autodiscover", settings.getAsBoolean("elasticsearch.autodiscover", false))
                .put("name", "importer") // prevents lookup of names.txt, we don't have it
                .put("client.transport.ignore_cluster_name", false) // ignore cluster name setting
                .put("client.transport.ping_timeout", settings.getAsTime("elasticsearch.timeout", TimeValue.timeValueSeconds(5))) //  ping timeout
                .put("client.transport.nodes_sampler_interval", settings.getAsTime("elasticsearch.timeout", TimeValue.timeValueSeconds(5))); // for sniff sampling
        // optional found.no transport plugin
        if (settings.get("transport.type") != null) {
            settingsBuilder.put("transport.type", settings.get("transport.type"));
        }
        // copy found.no transport settings
        Settings foundTransportSettings = settings.getAsSettings("transport.found");
        if (foundTransportSettings != null) {
            Map<String,String> foundTransportSettingsMap = foundTransportSettings.getAsMap();
            for (Map.Entry<String,String> entry : foundTransportSettingsMap.entrySet()) {
                settingsBuilder.put("transport.found." + entry.getKey(), entry.getValue());
            }
        }
        return ClientBuilder.builder()
                .put(settingsBuilder.build())
                .put(ClientBuilder.MAX_ACTIONS_PER_REQUEST, settings.getAsInt("max_bulk_actions", 10000))
                .put(ClientBuilder.MAX_CONCURRENT_REQUESTS, settings.getAsInt("max_concurrent_bulk_requests",
                        Runtime.getRuntime().availableProcessors() * 2))
                .put(ClientBuilder.MAX_VOLUME_PER_REQUEST, settings.getAsBytesSize("max_bulk_volume", ByteSizeValue.parseBytesSizeValue("10m", "")))
                .put(ClientBuilder.FLUSH_INTERVAL, settings.getAsTime("flush_interval", TimeValue.timeValueSeconds(5)))
                .setMetric(sinkMetric)
                .toBulkTransportClient();
    }

    private void createIndex(Settings settings, String index, String type) throws IOException {
        if (index == null) {
            return;
        }
        if (clientAPI.client() != null) {
            try {
                clientAPI.waitForCluster("YELLOW", TimeValue.timeValueSeconds(30));
                if (settings.getAsStructuredMap().containsKey("index_settings")) {
                    Settings indexSettings = settings.getAsSettings("index_settings");
                    Map<String,String> mappings = new HashMap<>();
                    if (type != null) {
                        Settings typeMapping = settings.getAsSettings("type_mapping");
                        XContentBuilder builder = jsonBuilder();
                        builder.startObject();
                        typeMapping.toXContent(builder, ToXContent.EMPTY_PARAMS);
                        builder.endObject();
                        mappings.put(type, builder.string());
                    }
                    logger.info("creating index {} type {} with mapping {}", index, type, mappings);
                    clientAPI.newIndex(index, indexSettings, mappings);
                    logger.info("index created");
                    long startRefreshInterval = indexSettings.getAsTime("bulk." + index + ".refresh_interval.start",
                                    TimeValue.timeValueMillis(-1L)).getMillis();
                    long stopRefreshInterval = indexSettings.getAsTime("bulk." + index + ".refresh_interval.stop",
                                    indexSettings.getAsTime("index.refresh_interval", TimeValue.timeValueSeconds(1))).getMillis();
                    logger.info("start bulk mode, refresh at start = {}, refresh at stop = {}", startRefreshInterval, stopRefreshInterval);
                    clientAPI.startBulk(index, startRefreshInterval, stopRefreshInterval);
                }
            } catch (Exception e) {
                if (!settings.getAsBoolean("ignoreindexcreationerror", false)) {
                    throw e;
                } else {
                    logger.warn("index creation error, but configured to ignore", e);
                }
            }
        }
    }

    private String resolveAlias(String alias) {
        if (clientAPI.client() == null) {
            return alias;
        }
        GetAliasesResponse getAliasesResponse = clientAPI.client().prepareExecute(GetAliasesAction.INSTANCE).setAliases(alias).execute().actionGet();
        if (!getAliasesResponse.getAliases().isEmpty()) {
            return getAliasesResponse.getAliases().keys().iterator().next().value;
        }
        return alias;
    }

}
