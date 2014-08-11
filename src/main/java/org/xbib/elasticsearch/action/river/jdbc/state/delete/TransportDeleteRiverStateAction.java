package org.xbib.elasticsearch.action.river.jdbc.state.delete;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.TransportMasterNodeOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.xbib.elasticsearch.action.river.jdbc.state.RiverStateService;

public class TransportDeleteRiverStateAction extends TransportMasterNodeOperationAction<DeleteRiverStateRequest, DeleteRiverStateResponse> {

    private final Injector injector;

    @Inject
    public TransportDeleteRiverStateAction(Settings settings, ThreadPool threadPool,
                                           ClusterService clusterService, TransportService transportService,
                                           Injector injector) {
        super(settings, transportService, clusterService, threadPool);
        this.injector = injector;
    }

    @Override
    public String transportAction() {
        return DeleteRiverStateAction.NAME;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected DeleteRiverStateRequest newRequest() {
        return new DeleteRiverStateRequest();
    }

    @Override
    protected DeleteRiverStateResponse newResponse() {
        return new DeleteRiverStateResponse();
    }

    @Override
    protected void masterOperation(DeleteRiverStateRequest request, ClusterState state, final ActionListener<DeleteRiverStateResponse> listener) throws ElasticsearchException {
        RiverStateService riverStateService = injector.getInstance(RiverStateService.class);
        riverStateService.unregisterRiver(new RiverStateService.UnregisterRiverStateRequest("delete_river_state[" + request.getRiverName() + "]", request.getRiverName())
                .masterNodeTimeout(request.masterNodeTimeout())
                .ackTimeout(request.timeout()), new ActionListener<ClusterStateUpdateResponse>() {
            @Override
            public void onResponse(ClusterStateUpdateResponse clusterStateUpdateResponse) {
                listener.onResponse(new DeleteRiverStateResponse(clusterStateUpdateResponse.isAcknowledged()));
            }

            @Override
            public void onFailure(Throwable e) {
                listener.onFailure(e);
            }
        });
    }

}
