package org.xbib.elasticsearch.action.river.jdbc.state.put;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedRequestBuilder;
import org.elasticsearch.client.ClusterAdminClient;
import org.xbib.elasticsearch.action.river.jdbc.state.RiverState;

public class PutRiverStateRequestBuilder extends AcknowledgedRequestBuilder<PutRiverStateRequest, PutRiverStateResponse, PutRiverStateRequestBuilder, ClusterAdminClient> {

    public PutRiverStateRequestBuilder(ClusterAdminClient client) {
        super(client, new PutRiverStateRequest());
    }

    public PutRiverStateRequestBuilder setRiverName(String riverName) {
        request.setRiverName(riverName);
        return this;
    }

    public PutRiverStateRequestBuilder setRiverType(String riverType) {
        request.setRiverType(riverType);
        return this;
    }

    public PutRiverStateRequestBuilder setRiverState(RiverState riverState) {
        request.setRiverState(riverState);
        return this;
    }

    @Override
    protected void doExecute(ActionListener<PutRiverStateResponse> listener) {
        client.execute(PutRiverStateAction.INSTANCE, request, listener);
    }
}
