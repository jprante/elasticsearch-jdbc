package org.xbib.elasticsearch.action.river.jdbc.state.delete;

import org.elasticsearch.action.admin.cluster.ClusterAction;
import org.elasticsearch.client.ClusterAdminClient;

public class DeleteRiverStateAction extends ClusterAction<DeleteRiverStateRequest, DeleteRiverStateResponse, DeleteRiverStateRequestBuilder> {

    public static final DeleteRiverStateAction INSTANCE = new DeleteRiverStateAction();

    public static final String NAME = "org.xbib.elasticsearch.action.river.state.delete.jdbc";

    private DeleteRiverStateAction() {
        super(NAME);
    }

    @Override
    public DeleteRiverStateRequestBuilder newRequestBuilder(ClusterAdminClient client) {
        return new DeleteRiverStateRequestBuilder(client);
    }

    @Override
    public DeleteRiverStateResponse newResponse() {
        return new DeleteRiverStateResponse();
    }
}
