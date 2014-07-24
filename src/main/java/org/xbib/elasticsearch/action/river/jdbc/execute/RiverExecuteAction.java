package org.xbib.elasticsearch.action.river.jdbc.execute;

import org.elasticsearch.action.admin.cluster.ClusterAction;
import org.elasticsearch.client.ClusterAdminClient;

public class RiverExecuteAction extends ClusterAction<RiverExecuteRequest, RiverExecuteResponse, RiverExecuteRequestBuilder> {

    public static final RiverExecuteAction INSTANCE = new RiverExecuteAction();

    public static final String NAME = "org.xbib.elasticsearch.action.river.execute.jdbc";

    private RiverExecuteAction() {
        super(NAME);
    }

    @Override
    public RiverExecuteRequestBuilder newRequestBuilder(ClusterAdminClient client) {
        return new RiverExecuteRequestBuilder(client);
    }

    @Override
    public RiverExecuteResponse newResponse() {
        return new RiverExecuteResponse();
    }
}
