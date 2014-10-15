package org.xbib.elasticsearch.action.plugin.jdbc.run;

import org.elasticsearch.action.admin.cluster.ClusterAction;
import org.elasticsearch.client.ClusterAdminClient;

public class RunRiverAction extends ClusterAction<RunRiverRequest, RunRiverResponse, RunRiverRequestBuilder> {

    public static final RunRiverAction INSTANCE = new RunRiverAction();

    public static final String NAME = "org.xbib.elasticsearch.action.plugin.jdbc.run";

    private RunRiverAction() {
        super(NAME);
    }

    @Override
    public RunRiverRequestBuilder newRequestBuilder(ClusterAdminClient client) {
        return new RunRiverRequestBuilder(client);
    }

    @Override
    public RunRiverResponse newResponse() {
        return new RunRiverResponse();
    }
}