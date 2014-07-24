package org.xbib.elasticsearch.action.river.jdbc.execute;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.nodes.NodesOperationRequestBuilder;
import org.elasticsearch.client.ClusterAdminClient;

public class RiverExecuteRequestBuilder extends NodesOperationRequestBuilder<RiverExecuteRequest, RiverExecuteResponse, RiverExecuteRequestBuilder> {

    public RiverExecuteRequestBuilder(ClusterAdminClient client) {
        super(client, new RiverExecuteRequest());
    }

    public RiverExecuteRequestBuilder setRiverType(String riverType) {
        request.setRiverType(riverType);
        return this;
    }

    public RiverExecuteRequestBuilder setRiverName(String riverName) {
        request.setRiverName(riverName);
        return this;
    }

    @Override
    protected void doExecute(ActionListener<RiverExecuteResponse> listener) {
        client.execute(RiverExecuteAction.INSTANCE, request, listener);
    }
}
