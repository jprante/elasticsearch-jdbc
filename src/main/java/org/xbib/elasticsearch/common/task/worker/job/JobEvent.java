package org.xbib.elasticsearch.common.task.worker.job;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.common.collect.Lists.newArrayList;
import static org.elasticsearch.common.xcontent.XContentParser.Token.END_ARRAY;
import static org.elasticsearch.common.xcontent.XContentParser.Token.END_OBJECT;
import static org.elasticsearch.common.xcontent.XContentParser.Token.FIELD_NAME;
import static org.elasticsearch.common.xcontent.XContentParser.Token.START_ARRAY;
import static org.elasticsearch.common.xcontent.XContentParser.Token.VALUE_NULL;

public class JobEvent implements ToXContent {

    private String nodeName;

    private Long timestamp;

    public JobEvent() {
    }

    public JobEvent nodeName(String nodeName) {
        this.nodeName = nodeName;
        return this;
    }

    public JobEvent timestamp(Long timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    public JobEvent fromXContent(XContentParser parser) throws IOException {
        //DateMathParser dateParser = new DateMathParser(Joda.forPattern("dateOptionalTime"), TimeUnit.MILLISECONDS);
        Long startTimestamp = null;
        String currentFieldName = null;
        Token token;
        while ((token = parser.nextToken()) != END_OBJECT) {
            if (token == FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue() || token == VALUE_NULL) {
                if ("started".equals(currentFieldName)) {
                    startTimestamp =  Long.parseLong(parser.text());
                }
            } else if (token == START_ARRAY) {
                List<String> values = newArrayList();
                while ((parser.nextToken()) != END_ARRAY) {
                    values.add(parser.text());
                }
            }
        }
        return new JobEvent().timestamp(startTimestamp);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject()
                .field("started", timestamp)
                .field("nodeName", nodeName)
                .endObject();
    }

    public String id() {
        StringBuilder sb = new StringBuilder();
        sb.append(nodeName).append(timestamp);
        return sb.toString();
    }

    @Override
    public int hashCode() {
        return id().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        JobEvent other = (JobEvent) obj;
        return id().equals(other.id());
    }

}