
package org.xbib.elasticsearch.river.jdbc.strategy.simple;

import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.index.VersionType;
import org.xbib.elasticsearch.plugin.jdbc.ControlKeys;
import org.xbib.elasticsearch.plugin.jdbc.IndexableObject;
import org.xbib.elasticsearch.plugin.jdbc.RiverContext;
import org.xbib.elasticsearch.plugin.jdbc.Values;
import org.xbib.elasticsearch.river.jdbc.RiverMouth;
import org.xbib.elasticsearch.support.client.Ingest;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
        this.index = index;
        return this;
    }

    @Override
    public SimpleRiverMouth setType(String type) {
        this.type = type;
        return this;
    }

    @Override
    public SimpleRiverMouth setId(String id) {
        this.id = id;
        return this;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public void index(IndexableObject object, boolean create) throws IOException {
        if (Strings.hasLength(object.index())) {
            this.index = object.index();
        }
        if (Strings.hasLength(object.type())) {
            this.type = object.type();
        }
        if (Strings.hasLength(object.id())) {
            setId(object.id());
        }

        if(context.ignoreNull()) {
            removeEmptyObjects(object.source());
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
        ingest.index(request);
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
        ingest.delete(request);
    }

    @Override
    public void flush() throws IOException {
        if (ingest != null) {
            ingest.flush();
        }
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            // do not shut down ingest object...  we need it for cleanup
            closed = true;
        }
    }

    private void removeEmptyObjects(Map map)
    {
        Iterator mapIterator = map.keySet().iterator();
        while (mapIterator.hasNext())
        {
            Object key = mapIterator.next();
            Object mapObject = map.get(key);
            if (mapObject == null)
            {
                mapIterator.remove();
            } else if (mapObject instanceof Values && ((Values) mapObject).isNull())
            {
                mapIterator.remove();
            }
            else if (mapObject instanceof Map)
            {
                Map innerMap = (Map) mapObject;
                removeEmptyObjects(innerMap);
                if (innerMap.size() == 0)
                {
                    mapIterator.remove();
                }
            }
            else if (mapObject instanceof List)
            {
                List innerList = (List) mapObject;
                removeEmptyObjects(innerList);
                if (innerList.size() == 0)
                {
                    mapIterator.remove();
                }
            }
        }
    }

    private void removeEmptyObjects(List list)
    {
        Iterator listIterator = list.iterator();
        while (listIterator.hasNext())
        {
            Object listObject = listIterator.next();
            if (listObject == null)
            {
                listIterator.remove();
            } else if (listObject instanceof Values && ((Values) listObject).isNull())
            {
                listIterator.remove();
            } else if (listObject instanceof List)
            {
                List innerList = (List) listObject;
                removeEmptyObjects(innerList);
                if (innerList.size() == 0)
                {
                    listIterator.remove();
                }
            } else if (listObject instanceof Map)
            {
                Map innerMap = (Map) listObject;
                removeEmptyObjects(innerMap);
                if (innerMap.size() == 0)
                {
                    listIterator.remove();
                }
            }
        }
    }

}
