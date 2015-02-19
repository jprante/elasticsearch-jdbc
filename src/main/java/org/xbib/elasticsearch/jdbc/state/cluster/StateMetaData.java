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
package org.xbib.elasticsearch.jdbc.state.cluster;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.xbib.elasticsearch.jdbc.state.State;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.common.collect.Lists.newLinkedList;

/**
 * Contains metadata about states
 */
public class StateMetaData implements MetaData.Custom {

    public static final String TYPE = "states";

    public static final Factory FACTORY = new Factory();

    private final ImmutableList<State> states;

    /**
     * Constructs new state metadata
     *
     * @param states list of states
     */
    public StateMetaData(State... states) {
        this.states = ImmutableList.copyOf(states);
    }

    /**
     * Returns list of current states
     *
     * @return list of states
     */
    public ImmutableList<State> getStates() {
        return this.states;
    }

    /**
     * Returns a state with a given name or null if such doesn't exist
     *
     * @param name name
     * @return metadata
     */
    public ImmutableList<State> getStates(String name) {
        ImmutableList.Builder<State> stateBuilder = ImmutableList.builder();
        for (State state : states) {
            if (("*".equals(name) || name.equals(state.getName()))) {
                stateBuilder.add(state);
            }
        }
        return stateBuilder.build();
    }

    /**
     * State metadata factory
     */
    public static class Factory extends MetaData.Custom.Factory<StateMetaData> {

        @Override
        public String type() {
            return TYPE;
        }

        @Override
        public StateMetaData readFrom(StreamInput in) throws IOException {
            State[] state = new State[in.readVInt()];
            for (int i = 0; i < state.length; i++) {
                state[i] = new State();
                state[i].readFrom(in);
            }
            return new StateMetaData(state);
        }

        @Override
        public void writeTo(StateMetaData stateMetaData, StreamOutput out) throws IOException {
            out.writeVInt(stateMetaData.getStates().size());
            for (State state : stateMetaData.getStates()) {
                state.writeTo(out);
            }
        }

        @Override
        public StateMetaData fromXContent(XContentParser parser) throws IOException {
            XContentParser.Token token = parser.nextToken();
            List<State> stateList = newLinkedList();
            if (token == XContentParser.Token.FIELD_NAME) {
                String name = parser.currentName();
                if (parser.nextToken() != XContentParser.Token.START_ARRAY) {
                    throw new ElasticsearchParseException("failed to parse state at [" + name + "], expected array");
                }
            }
            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                State state = new State();
                state.fromXContent(parser);
                stateList.add(state);
            }
            return new StateMetaData(stateList.toArray(new State[stateList.size()]));
        }

        @Override
        public void toXContent(StateMetaData stateMetaData, XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.startArray("states");
            for (State state : stateMetaData.getStates()) {
                state.toXContent(builder, params);
            }
            builder.endArray();
        }

    }

}