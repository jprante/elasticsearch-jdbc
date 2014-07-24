package org.xbib.elasticsearch.action.river.jdbc.state.put;

import org.elasticsearch.action.admin.cluster.ClusterAction;
import org.elasticsearch.client.ClusterAdminClient;

public class PutRiverStateAction extends ClusterAction<PutRiverStateRequest, PutRiverStateResponse, PutRiverStateRequestBuilder> {

    public static final PutRiverStateAction INSTANCE = new PutRiverStateAction();

    public static final String NAME = "org.xbib.elasticsearch.action.river.state.put.jdbc";

    private PutRiverStateAction() {
        super(NAME);
    }

    @Override
    public PutRiverStateRequestBuilder newRequestBuilder(ClusterAdminClient client) {
        return new PutRiverStateRequestBuilder(client);
    }

    @Override
    public PutRiverStateResponse newResponse() {
        return new PutRiverStateResponse();
    }
}
