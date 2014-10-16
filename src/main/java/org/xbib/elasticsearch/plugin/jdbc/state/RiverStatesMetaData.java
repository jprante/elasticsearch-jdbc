/*
 * Copyright (C) 2014 Jörg Prante
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
package org.xbib.elasticsearch.plugin.jdbc.state;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.common.collect.Lists.newLinkedList;

/**
 * Contains metadata about registered rivers
 */
public class RiverStatesMetaData implements MetaData.Custom {

    public static final String TYPE = "riverstates";

    public static final Factory FACTORY = new Factory();

    private final ImmutableList<RiverState> riverStates;

    /**
     * Constructs new river state metadata
     *
     * @param riverStates list of river states
     */
    public RiverStatesMetaData(RiverState... riverStates) {
        this.riverStates = ImmutableList.copyOf(riverStates);
    }

    /**
     * Returns list of current river states
     *
     * @return list of river states
     */
    public ImmutableList<RiverState> getRiverStates() {
        return this.riverStates;
    }

    /**
     * Returns a river state with a given name or null if such river doesn't exist
     *
     * @param name name of river
     * @return river metadata
     */
    public ImmutableList<RiverState> getRiverStates(String name, String type) {
        ImmutableList.Builder<RiverState> riverStatesBuilder = ImmutableList.builder();
        for (RiverState riverState : riverStates) {
            if (("*".equals(name) || name.equals(riverState.getName())) &&
                    ("*".equals(type) || type.equals(riverState.getType()))) {
                riverStatesBuilder.add(riverState);
            }
        }
        return riverStatesBuilder.build();
    }

    /**
     * River state metadata factory
     */
    public static class Factory extends MetaData.Custom.Factory<RiverStatesMetaData> {

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
        public void writeTo(RiverStatesMetaData riverStatesMetaData, StreamOutput out) throws IOException {
            out.writeVInt(riverStatesMetaData.getRiverStates().size());
            for (RiverState river : riverStatesMetaData.getRiverStates()) {
                river.writeTo(out);
            }
        }

        @Override
        public RiverStatesMetaData fromXContent(XContentParser parser) throws IOException {
            XContentParser.Token token = parser.nextToken();
            List<RiverState> riverStateList = newLinkedList();
            if (token == XContentParser.Token.FIELD_NAME) {
                String name = parser.currentName();
                if (parser.nextToken() != XContentParser.Token.START_ARRAY) {
                    throw new ElasticsearchParseException("failed to parse river state at [" + name + "], expected array");
                }
            }
            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                RiverState riverState = new RiverState();
                riverState.fromXContent(parser);
                riverStateList.add(riverState);
            }
            return new RiverStatesMetaData(riverStateList.toArray(new RiverState[riverStateList.size()]));
        }

        @Override
        public void toXContent(RiverStatesMetaData riverStatesMetaData, XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.startArray("states");
            for (RiverState riverState : riverStatesMetaData.getRiverStates()) {
                riverState.toXContent(builder, params);
            }
            builder.endArray();
        }

    }

}