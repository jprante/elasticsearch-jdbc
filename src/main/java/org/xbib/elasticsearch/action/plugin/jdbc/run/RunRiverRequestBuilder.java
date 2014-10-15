package org.xbib.elasticsearch.action.plugin.jdbc.run;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.nodes.NodesOperationRequestBuilder;
import org.elasticsearch.client.ClusterAdminClient;

public class RunRiverRequestBuilder extends NodesOperationRequestBuilder<RunRiverRequest, RunRiverResponse, RunRiverRequestBuilder> {

    public RunRiverRequestBuilder(ClusterAdminClient client) {
        super(client, new RunRiverRequest());
    }

    public RunRiverRequestBuilder setRiverType(String riverType) {
        request.setRiverType(riverType);
        return this;
    }

    public RunRiverRequestBuilder setRiverName(String riverName) {
        request.setRiverName(riverName);
        return this;
    }

    @Override
    protected void doExecute(ActionListener<RunRiverResponse> listener) {
        client.execute(RunRiverAction.INSTANCE, request, listener);
    }
}