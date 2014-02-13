
package org.xbib.elasticsearch.river.jdbc;

import java.io.IOException;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.joda.DateMathParser;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.indices.IndexMissingException;

import static org.elasticsearch.client.Requests.indexRequest;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.common.xcontent.XContentParser.Token.END_OBJECT;
import static org.elasticsearch.common.xcontent.XContentParser.Token.FIELD_NAME;
import static org.elasticsearch.common.xcontent.XContentParser.Token.START_OBJECT;
import static org.elasticsearch.common.xcontent.XContentParser.Token.VALUE_BOOLEAN;
import static org.elasticsearch.common.xcontent.XContentParser.Token.VALUE_NULL;
import static org.elasticsearch.common.xcontent.XContentParser.Token.VALUE_NUMBER;
import static org.elasticsearch.common.xcontent.XContentParser.Token.VALUE_STRING;

public class RiverState implements ToXContent {

    private final ESLogger logger = ESLoggerFactory.getLogger(RiverState.class.getName());

    private String name;

    private Long started;

    private Long timestamp;

    private Long counter;

    private boolean active;

    private Map<String,Object> custom;

    private String index;

    private String type;

    private String id;

    public RiverState setIndex(String index) {
        this.index = index;
        return this;
    }

    public RiverState setType(String type) {
        this.type = type;
        return this;
    }

    public RiverState setId(String id) {
        this.id = id;
        return this;
    }

    public RiverState name(String name) {
        this.name = name;
        return this;
    }

    public String name() {
        return name;
    }

    public RiverState counter(Long counter) {
        this.counter = counter;
        return this;
    }

    public Long counter() {
        return counter;
    }

    public RiverState started(Long started) {
        this.started = started;
        return this;
    }

    public Long started() {
        return started;
    }

    public RiverState timestamp(Long timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    public Long timestamp() {
        return timestamp;
    }

    public RiverState active(boolean active) {
        this.active = active;
        return this;
    }

    public boolean active() {
        return active;
    }

    public RiverState custom(Map<String,Object> custom) {
        this.custom = custom;
        return this;
    }

    public Map<String,Object> custom() {
        return custom;
    }

    public void save(Client client) throws IOException {
        XContentBuilder builder = jsonBuilder();
        builder = toXContent(builder, ToXContent.EMPTY_PARAMS);
        logger.debug("save state={}",builder.string());
        client.index(indexRequest().index(index).type(type).id(id)
                .source(builder.string()))
                .actionGet();
    }

    public void load(Client client) throws IOException {
        GetResponse get = null;
        try {
            client.admin().indices().prepareRefresh(index).execute().actionGet();
            get = client.prepareGet(index, type, id).execute().actionGet();
        } catch (IndexMissingException e) {
            logger.warn("river state missing: {}/{}/{}", index, type, id);
        }
        if (get != null && get.isExists()) {
            logger.debug("load state={}", get.getSourceAsString());
            XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                    .createParser(get.getSourceAsBytes());
            fromXContent(parser);
        } else {
            counter = 0L;
        }
    }

    public void fromXContent(XContentParser parser) throws IOException {
        DateMathParser dateParser = new DateMathParser(Joda.forPattern("dateOptionalTime"), TimeUnit.MILLISECONDS);
        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != END_OBJECT) {
            if (token == null) {
                break;
            } else if (token == FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == VALUE_NULL || token == VALUE_STRING
                    || token == VALUE_BOOLEAN || token == VALUE_NUMBER) {
                if ("name".equals(currentFieldName)) {
                    name(parser.text());
                } else if ("started".equals(currentFieldName)) {
                    started(dateParser.parse(parser.text(), 0));
                } else if ("timestamp".equals(currentFieldName)) {
                    timestamp(dateParser.parse(parser.text(), 0));
                } else if ("counter".equals(currentFieldName)) {
                    counter(parser.longValue());
                } else if ("active".equals(currentFieldName)) {
                    active(parser.booleanValue());
                }
            } else if (token == START_OBJECT) {
                if ("custom".equals(currentFieldName)) {
                    custom(parser.map());
                } // else skip
            }
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject()
                .field("name", name)
                .field("started", new Date(started))
                .field("timestamp", new Date(timestamp))
                .field("counter", counter)
                .field("active", active)
                .field("custom", custom)
            .endObject();
        return builder;
    }
}
