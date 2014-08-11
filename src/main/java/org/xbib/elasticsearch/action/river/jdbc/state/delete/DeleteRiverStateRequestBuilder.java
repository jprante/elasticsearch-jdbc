package org.xbib.elasticsearch.action.river.jdbc.state.delete;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedRequestBuilder;
import org.elasticsearch.client.ClusterAdminClient;
import org.elasticsearch.client.internal.InternalGenericClient;

public class DeleteRiverStateRequestBuilder extends AcknowledgedRequestBuilder<DeleteRiverStateRequest, DeleteRiverStateResponse, DeleteRiverStateRequestBuilder> {

    public DeleteRiverStateRequestBuilder(ClusterAdminClient client) {
        super((InternalGenericClient)client, new DeleteRiverStateRequest());
    }

    public DeleteRiverStateRequestBuilder setRiverName(String riverName) {
        request.setRiverName(riverName);
        return this;
    }

    @Override
    protected void doExecute(ActionListener<DeleteRiverStateResponse> listener) {
        ((ClusterAdminClient)client).execute(DeleteRiverStateAction.INSTANCE, request, listener);
    }
}
