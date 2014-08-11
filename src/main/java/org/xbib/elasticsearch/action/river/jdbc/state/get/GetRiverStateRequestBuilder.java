package org.xbib.elasticsearch.action.river.jdbc.state.get;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.MasterNodeReadOperationRequestBuilder;
import org.elasticsearch.client.ClusterAdminClient;
import org.elasticsearch.client.internal.InternalGenericClient;

public class GetRiverStateRequestBuilder extends MasterNodeReadOperationRequestBuilder<GetRiverStateRequest, GetRiverStateResponse, GetRiverStateRequestBuilder> {

    public GetRiverStateRequestBuilder(ClusterAdminClient client) {
        super((InternalGenericClient)client, new GetRiverStateRequest());
    }

    public GetRiverStateRequestBuilder setRiverName(String riverName) {
        request.setRiverName(riverName);
        return this;
    }

    public GetRiverStateRequestBuilder setRiverType(String riverType) {
        request.setRiverType(riverType);
        return this;
    }

    @Override
    protected void doExecute(ActionListener<GetRiverStateResponse> listener) {
        ((ClusterAdminClient)client).execute(GetRiverStateAction.INSTANCE, request, listener);
    }
}
