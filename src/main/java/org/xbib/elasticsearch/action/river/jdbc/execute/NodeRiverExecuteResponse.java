package org.xbib.elasticsearch.action.river.jdbc.execute;

import org.elasticsearch.action.support.nodes.NodeOperationResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class NodeRiverExecuteResponse extends NodeOperationResponse {

    private boolean executed;

    NodeRiverExecuteResponse() {
    }

    public NodeRiverExecuteResponse(DiscoveryNode node) {
        super(node);
    }

    public NodeRiverExecuteResponse setExecuted(boolean b) {
        this.executed = b;
        return this;
    }

    public boolean isExecuted() {
        return executed;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        executed = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(executed);
    }

}
