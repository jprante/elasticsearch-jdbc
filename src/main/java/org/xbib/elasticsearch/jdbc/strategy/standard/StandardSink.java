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
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.xbib.elasticsearch.common.metrics.SinkMetric;
import org.xbib.elasticsearch.common.util.ControlKeys;
import org.xbib.elasticsearch.common.util.IndexableObject;
import org.xbib.elasticsearch.jdbc.strategy.Sink;
import org.xbib.elasticsearch.helper.client.Ingest;
import org.xbib.elasticsearch.helper.client.IngestFactory;
import org.xbib.elasticsearch.helper.client.transport.BulkTransportClient;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Standard sink implementation. This implementation uses bulk processing,
 * index name housekeeping (with replica/refresh), and metrics. It understands
 * _version, _routing, _timestamp, _parent, and _ttl metadata.
 */
public class StandardSink<C extends StandardContext> implements Sink<C> {

    private final static Logger logger = LogManager.getLogger("importer.jdbc.sink.standard");

    protected C context;

    private IngestFactory ingestFactory;

    protected Ingest ingest;

    protected Settings indexSettings;

    protected Map<String, String> indexMappings;

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
        if (ingest == null) {
            try {
                ingest = createIngestFactory(context.getSettings()).create();
            } catch (IOException e) {

            }
        }
        return this;
    }


    @Override
    public SinkMetric getMetric() {
        return sinkMetric;
    }

    @Override
    public synchronized void beforeFetch() throws IOException {
        if (ingest == null) {
            logger.warn("no ingest found");
            return;
        }
        if (ingest.client() != null) {
                logger.info("creating index {} with settings = {} and mappings = {}",
                        index,
                        indexSettings != null ? indexSettings.getAsMap() : "",
                        indexMappings != null ? indexMappings : "");
                try {
                    ingest.newIndex(index, indexSettings, indexMappings);
                } catch (IndexAlreadyExistsException e) {
                    logger.warn(e.getMessage());
                }
        }
        long startRefreshInterval = indexSettings != null ?
                indexSettings.getAsTime("bulk." + index + ".refresh_interval.start",
                        TimeValue.timeValueSeconds(-1)).getMillis() : -1L;
        long stopRefreshInterval = indexSettings != null ?
                indexSettings.getAsTime("bulk." + index + ".refresh_interval.stop",
                        indexSettings.getAsTime("index.refresh_interval", TimeValue.timeValueSeconds(1))).getMillis() : 1000L;
        ingest.startBulk(index, startRefreshInterval, stopRefreshInterval);
    }

    @Override
    public synchronized void afterFetch() throws IOException {
        if (ingest == null) {
            return;
        }
        logger.debug("afterFetch: flush ingest");
        flushIngest();
        logger.debug("afterFetch: stop bulk");
        ingest.stopBulk(index);
        logger.debug("afterFetch: refresh index");
        ingest.refreshIndex(index);
        logger.debug("afterFetch: before ingest shutdown");
        ingest.shutdown();
        ingest = null;
        logger.debug("afterFetch: after ingest shutdown");
    }

    @Override
    public synchronized void shutdown() {
        if (ingest == null) {
            return;
        }
        try {
            logger.info("shutdown in progress");
            flushIngest();
            ingest.stopBulk(index);
            ingest.shutdown();
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }

    @Override
    public StandardSink setIndexSettings(Settings indexSettings) {
        this.indexSettings = indexSettings;
        return this;
    }

    @Override
    public StandardSink setTypeMapping(Map<String, String> typeMapping) {
        this.indexMappings = typeMapping;
        return this;
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
        if (ingest == null) {
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
        ingest.bulkIndex(request);
    }

    @Override
    public void delete(IndexableObject object) {
        if (ingest == null) {
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
        ingest.bulkDelete(request);
    }

    @Override
    public void update(IndexableObject object) throws IOException {
        if (ingest == null) {
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
        ingest.bulkUpdate(request);
    }

    @Override
    public void flushIngest() throws IOException {
        if (ingest == null) {
            return;
        }
        ingest.flushIngest();
        // wait for all outstanding bulk requests before continuing. Estimation is 60 seconds
        try {
            ingest.waitForResponses(TimeValue.timeValueSeconds(60));
        } catch (InterruptedException e) {
            logger.warn("interrupted while waiting for responses");
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            logger.warn("exception while executing", e);
        }
    }

    @Override
    public StandardSink setIngestFactory(IngestFactory ingestFactory) {
        this.ingestFactory = ingestFactory;
        return this;
    }

    @Override
    public IngestFactory getIngestFactory() {
        return ingestFactory;
    }

    private IngestFactory createIngestFactory(final Settings settings) {
        return new IngestFactory() {
            @Override
            public Ingest create() throws IOException {
                Integer maxbulkactions = settings.getAsInt("max_bulk_actions", 10000);
                Integer maxconcurrentbulkrequests = settings.getAsInt("max_concurrent_bulk_requests",
                        Runtime.getRuntime().availableProcessors() * 2);
                ByteSizeValue maxvolume = settings.getAsBytesSize("max_bulk_volume", ByteSizeValue.parseBytesSizeValue("10m", ""));
                TimeValue flushinterval = settings.getAsTime("flush_interval", TimeValue.timeValueSeconds(5));
                BulkTransportClient ingest = new BulkTransportClient();
                Settings.Builder settingsBuilder = Settings.settingsBuilder()
                        .put("cluster.name", settings.get("elasticsearch.cluster", "elasticsearch"))
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
                try {
                    ingest.maxActionsPerRequest(maxbulkactions)
                            .maxConcurrentRequests(maxconcurrentbulkrequests)
                            .maxVolumePerRequest(maxvolume)
                            .flushIngestInterval(flushinterval)
                            .init(settingsBuilder.build(), sinkMetric);
                } catch (Exception e) {
                    logger.error("ingest not properly build, shutting down ingest",e);
                    ingest.shutdown();
                    ingest = null;
                }
                return ingest;
            }
        };
    }

}
