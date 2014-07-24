package org.xbib.elasticsearch.action.river.jdbc.state.get;

import org.elasticsearch.action.admin.cluster.ClusterAction;
import org.elasticsearch.client.ClusterAdminClient;

public class GetRiverStateAction extends ClusterAction<GetRiverStateRequest, GetRiverStateResponse, GetRiverStateRequestBuilder> {

    public static final GetRiverStateAction INSTANCE = new GetRiverStateAction();

    public static final String NAME = "org.xbib.elasticsearch.action.river.state.get.jdbc";

    private GetRiverStateAction() {
        super(NAME);
    }

    @Override
    public GetRiverStateRequestBuilder newRequestBuilder(ClusterAdminClient client) {
        return new GetRiverStateRequestBuilder(client);
    }

    @Override
    public GetRiverStateResponse newResponse() {
        return new GetRiverStateResponse();
    }
}
