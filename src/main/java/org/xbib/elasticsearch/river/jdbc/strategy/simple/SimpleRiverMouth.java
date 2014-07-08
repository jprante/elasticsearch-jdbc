package org.xbib.elasticsearch.river.jdbc.strategy.simple;

import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.joda.time.format.DateTimeFormat;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.VersionType;
import org.xbib.elasticsearch.plugin.jdbc.ControlKeys;
import org.xbib.elasticsearch.plugin.jdbc.IndexableObject;
import org.xbib.elasticsearch.plugin.jdbc.RiverContext;
import org.xbib.elasticsearch.river.jdbc.RiverMouth;
import org.xbib.elasticsearch.support.client.Ingest;

import java.io.IOException;

/**
 * Simple river mouth
 */
public class SimpleRiverMouth implements RiverMouth {

    private final ESLogger logger = ESLoggerFactory.getLogger(SimpleRiverMouth.class.getName());

    protected RiverContext context;

    protected Ingest ingest;

    protected String index;

    protected String type;

    protected String id;

    private boolean timeWindowed;

    private volatile boolean closed;

    protected ESLogger logger() {
        return logger;
    }

    @Override
    public String strategy() {
        return "simple";
    }

    @Override
    public SimpleRiverMouth setRiverContext(RiverContext context) {
        this.context = context;
        return this;
    }

    @Override
    public SimpleRiverMouth setIngest(Ingest ingest) {
        this.ingest = ingest;
        return this;
    }

    @Override
    public SimpleRiverMouth setIndex(String index) {
        this.index = timeWindowed ? DateTimeFormat.forPattern(index).print(new DateTime()) : index;
        return this;
    }

    public String getIndex() {
        return index;
    }

    @Override
    public SimpleRiverMouth setType(String type) {
        this.type = type;
        return this;
    }

    public String getType() {
        return type;
    }

    @Override
    public SimpleRiverMouth setId(String id) {
        this.id = id;
        return this;
    }

    @Override
    public SimpleRiverMouth setTimeWindowed(boolean timeWindowed) {
        this.timeWindowed = timeWindowed;
        return this;
    }

    public boolean isTimeWindowed() {
        return timeWindowed;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public void index(IndexableObject object, boolean create) throws IOException {
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
        if (ingest != null) {
            ingest.index(request);
        }
    }

    @Override
    public void delete(IndexableObject object) {
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
        if (ingest != null) {
            ingest.delete(request);
        }
    }

    @Override
    public void flush() throws IOException {
        if (ingest != null) {
            ingest.flush();
            // wait for all outstanding bulk requests before continue with river
            try {
                ingest.waitForResponses(TimeValue.timeValueSeconds(60));
            } catch (InterruptedException e) {
                // ignore
            }
        }
    }

    @Override
    public void close() throws IOException {
        // keep open, do not close or shut down ingest object here...  we need it for cleanup
    }

}
