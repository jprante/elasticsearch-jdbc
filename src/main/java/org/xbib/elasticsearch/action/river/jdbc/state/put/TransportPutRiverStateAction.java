package org.xbib.elasticsearch.action.river.jdbc.state.put;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.TransportMasterNodeOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.xbib.elasticsearch.action.river.jdbc.state.RiverStateService;

public class TransportPutRiverStateAction extends TransportMasterNodeOperationAction<PutRiverStateRequest, PutRiverStateResponse> {

    private final RiverStateService riverStateService;

    @Inject
    public TransportPutRiverStateAction(Settings settings, ThreadPool threadPool,
                                        ClusterService clusterService, TransportService transportService,
                                        RiverStateService riverStateService) {
        super(settings, PutRiverStateAction.NAME, transportService, clusterService, threadPool);
        this.riverStateService = riverStateService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected PutRiverStateRequest newRequest() {
        return new PutRiverStateRequest();
    }

    @Override
    protected PutRiverStateResponse newResponse() {
        return new PutRiverStateResponse();
    }

    @Override
    protected void masterOperation(PutRiverStateRequest request, ClusterState state, final ActionListener<PutRiverStateResponse> listener) throws ElasticsearchException {
        riverStateService.registerRiver(new RiverStateService.RegisterRiverStateRequest("put_river_state[" + request.getRiverName() + "]", request.getRiverName(), request.getRiverType())
                .riverState(request.getRiverState())
                .masterNodeTimeout(request.masterNodeTimeout())
                .ackTimeout(request.ackTimeout()), new ActionListener<ClusterStateUpdateResponse>() {
            @Override
            public void onResponse(ClusterStateUpdateResponse clusterStateUpdateResponse) {
                listener.onResponse(new PutRiverStateResponse(clusterStateUpdateResponse.isAcknowledged()));
            }

            @Override
            public void onFailure(Throwable e) {
                listener.onFailure(e);
            }
        });
    }

}
