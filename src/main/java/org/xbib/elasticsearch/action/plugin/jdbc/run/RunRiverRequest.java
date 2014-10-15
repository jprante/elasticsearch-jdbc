package org.xbib.elasticsearch.action.plugin.jdbc.run;

import org.elasticsearch.action.support.nodes.NodesOperationRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class RunRiverRequest extends NodesOperationRequest<RunRiverRequest> {

    private String riverType;

    private String riverName;

    public RunRiverRequest setRiverType(String riverType) {
        this.riverType = riverType;
        return this;
    }

    public String getRiverType() {
        return riverType;
    }

    public RunRiverRequest setRiverName(String riverName) {
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