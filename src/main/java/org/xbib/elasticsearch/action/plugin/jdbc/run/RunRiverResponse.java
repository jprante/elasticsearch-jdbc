package org.xbib.elasticsearch.action.plugin.jdbc.run;

import org.elasticsearch.action.support.nodes.NodesOperationResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public class RunRiverResponse extends NodesOperationResponse implements ToXContent {

    private boolean[] executed;

    public RunRiverResponse() {
    }

    public RunRiverResponse setExecuted(boolean[] b) {
        this.executed = b;
        return this;
    }

    public boolean[] isExecuted() {
        return executed;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("executed", executed);
        return builder;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        int len = in.readInt();
        executed = new boolean[len];
        for (int i = 0; i < len; i++) {
            executed[i] = in.readBoolean();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeInt(executed.length);
        for (boolean b : executed) {
            out.writeBoolean(b);
        }
    }

}