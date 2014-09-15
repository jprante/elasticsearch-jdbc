package org.xbib.elasticsearch.action.river.jdbc.state;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.loader.SettingsLoader;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.collect.Maps.newHashMap;

/**
 * Contains metadata about registered river states
 */
public class RiverStatesMetaData implements MetaData.Custom {

    public static final String TYPE = "rivers";

    public static final Factory FACTORY = new Factory();

    private final ImmutableList<RiverState> rivers;

    /**
     * Constructs new river metadata
     *
     * @param rivers list of rivers
     */
    public RiverStatesMetaData(RiverState... rivers) {
        this.rivers = ImmutableList.copyOf(rivers);
    }

    /**
     * Returns list of currently registered rivers
     *
     * @return list of rivers
     */
    public ImmutableList<RiverState> rivers() {
        return this.rivers;
    }

    /**
     * Returns a river state with a given name or null if such river doesn't exist
     *
     * @param name name of river
     * @return river metadata
     */
    public RiverState river(String name) {
        for (RiverState river : rivers) {
            if (name.equals(river.getName())) {
                return river;
            }
        }
        return null;
    }

    /**
     * River state metadata factory
     */
    public static class Factory implements MetaData.Custom.Factory<RiverStatesMetaData> {

        @Override
        public String type() {
            return TYPE;
        }

        @Override
        public RiverStatesMetaData readFrom(StreamInput in) throws IOException {
            RiverState[] river = new RiverState[in.readVInt()];
            for (int i = 0; i < river.length; i++) {
                river[i] = new RiverState();
                river[i].readFrom(in);
            }
            return new RiverStatesMetaData(river);
        }

        @Override
        public void writeTo(RiverStatesMetaData rivers, StreamOutput out) throws IOException {
            out.writeVInt(rivers.rivers().size());
            for (RiverState river : rivers.rivers()) {
                river.writeTo(out);
            }
        }

        @Override
        public RiverStatesMetaData fromXContent(XContentParser parser) throws IOException {
            XContentParser.Token token;
            List<RiverState> river = new LinkedList<RiverState>();
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    String name = parser.currentName();
                    if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                        throw new ElasticsearchParseException("failed to parse river [" + name + "], expected object");
                    }
                    String type = null;
                    Settings settings = ImmutableSettings.EMPTY;
                    Map<String,Object> map = newHashMap();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            String currentFieldName = parser.currentName();
                            switch (currentFieldName) {
                                case "type":
                                    if (parser.nextToken() != XContentParser.Token.VALUE_STRING) {
                                        throw new ElasticsearchParseException("failed to parse river [" + name + "], unknown type");
                                    }
                                    type = parser.text();
                                    break;
                                case "settings":
                                    if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                                        throw new ElasticsearchParseException("failed to parse river [" + name + "], incompatible params");
                                    }
                                    settings = ImmutableSettings.settingsBuilder().put(SettingsLoader.Helper.loadNestedFromMap(parser.mapOrdered())).build();
                                    break;
                                case "map":
                                    if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                                        throw new ElasticsearchParseException("failed to parse river [" + name + "], incompatible params");
                                    }
                                    map = parser.mapOrdered();
                                    break;
                                default:
                                    throw new ElasticsearchParseException("failed to parse river [" + name + "], unknown field [" + currentFieldName + "]");
                            }
                        } else {
                            throw new ElasticsearchParseException("failed to parse river [" + name + "]");
                        }
                    }
                    if (type == null) {
                        throw new ElasticsearchParseException("failed to parse river [" + name + "], missing river type");
                    }
                    river.add(new RiverState(name, type).setSettings(settings).setMap(map));
                } else {
                    throw new ElasticsearchParseException("failed to parse rivers");
                }
            }
            return new RiverStatesMetaData(river.toArray(new RiverState[river.size()]));
        }

        @Override
        public void toXContent(RiverStatesMetaData customIndexMetaData, XContentBuilder builder, ToXContent.Params params) throws IOException {
            for (RiverState river : customIndexMetaData.rivers()) {
                toXContent(river, builder, params);
            }
        }

        public void toXContent(RiverState river, XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.startObject(river.getName(), XContentBuilder.FieldCaseConversion.NONE);
            builder.field("type", river.getType());
            builder.startObject("settings");
            for (Map.Entry<String, String> settingEntry : river.getSettings().getAsMap().entrySet()) {
                builder.field(settingEntry.getKey(), settingEntry.getValue());
            }
            builder.endObject();
            builder.field("map").map(river.getMap());
            builder.endObject();
        }

        @Override
        public boolean isPersistent() {
            return true;
        }

    }

}
