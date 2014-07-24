package org.xbib.elasticsearch.action.river.jdbc.execute;

import org.elasticsearch.action.support.nodes.NodeOperationRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class NodeRiverExecuteRequest extends NodeOperationRequest {

    private String riverType;

    private String riverName;

    NodeRiverExecuteRequest() {
    }

    public NodeRiverExecuteRequest(String nodeId, RiverExecuteRequest request) {
        super(request, nodeId);
        this.riverName = request.getRiverName();
        this.riverType = request.getRiverType();
    }

    public NodeRiverExecuteRequest setRiverType(String riverType) {
        this.riverType = riverType;
        return this;
    }

    public String getRiverType() {
        return riverType;
    }

    public NodeRiverExecuteRequest setRiverName(String riverName) {
        this.riverName = riverName;
        return this;
    }

    public String getRiverName() {
        return riverName;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        this.riverName = in.readString();
        this.riverType = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(riverName);
        out.writeString(riverType);
    }
}
