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
package org.xbib.elasticsearch.action.plugin.jdbc.state.get;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.xbib.elasticsearch.plugin.jdbc.state.RiverState;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class GetRiverStateResponse extends ActionResponse implements ToXContent {

    private ImmutableList<RiverState> states = ImmutableList.of();

    public GetRiverStateResponse() {
    }

    public GetRiverStateResponse(ImmutableList<RiverState> riverStates) {
        if (riverStates != null) {
            states = riverStates;
        }
    }

    public RiverState getRiverState() {
        if (states == null || states.isEmpty()) {
            return null;
        } else {
            return states.get(0);
        }
    }

    public ImmutableList<RiverState> getRiverStates() {
        return states;
    }

    public boolean exists(String name, String type) {
        if (states != null && name != null && type != null) {
            for (RiverState state : states) {
                if (state != null && name.equals(state.getName()) && type.equals(state.getType())) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        int len = in.readInt();
        ImmutableList.Builder<RiverState> builder = ImmutableList.builder();
        for (int i = 0; i < len; i++) {
            RiverState rs = new RiverState();
            rs.readFrom(in);
            builder.add(rs);
        }
        states = builder.build();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (states == null) {
            out.writeInt(0);
        } else {
            out.writeInt(states.size());
            for (RiverState rs : states) {
                rs.writeTo(out);
            }
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("state", states);
        return builder;
    }

    @Override
    public String toString() {
        try {
            XContentBuilder builder = jsonBuilder();
            builder.startObject();
            builder = toXContent(builder, EMPTY_PARAMS);
            builder.endObject();
            return builder.string();
        } catch (IOException e) {
            return "";
        }
    }

}