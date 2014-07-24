package org.xbib.elasticsearch.action.river.jdbc.state.get;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.xbib.elasticsearch.action.river.jdbc.state.RiverState;

import java.io.IOException;

public class GetRiverStateResponse extends ActionResponse implements ToXContent {

    private GetRiverStateRequest getRiverStateRequest;

    private ImmutableList<RiverState> states;

    public GetRiverStateResponse() {
        states = ImmutableList.of();
    }

    public GetRiverStateResponse(GetRiverStateRequest request, ImmutableList<RiverState> riverStates) {
        getRiverStateRequest = request;
        states = riverStates;
    }

    public RiverState getState() {
        if (states == null || states.isEmpty()) {
            return new RiverState(getRiverStateRequest.getRiverName(), getRiverStateRequest.getRiverType());
        } else {
            return states.get(0);
        }
    }

    public ImmutableList<RiverState> getStates() {
        return states;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("state", states);
        return builder;
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
        out.writeInt(states.size());
        for (RiverState rs : states) {
            rs.writeTo(out);
        }
    }

}
