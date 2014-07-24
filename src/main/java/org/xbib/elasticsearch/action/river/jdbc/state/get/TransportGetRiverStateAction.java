package org.xbib.elasticsearch.action.river.jdbc.state.get;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.TransportMasterNodeReadOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.xbib.elasticsearch.action.river.jdbc.state.RiverState;
import org.xbib.elasticsearch.action.river.jdbc.state.RiverStatesMetaData;
import org.xbib.elasticsearch.action.river.jdbc.state.StatefulRiver;
import org.xbib.elasticsearch.support.river.RiverHelper;

import java.util.Map;

public class TransportGetRiverStateAction extends TransportMasterNodeReadOperationAction<GetRiverStateRequest, GetRiverStateResponse> {

    private final Injector injector;

    @Inject
    public TransportGetRiverStateAction(Settings settings, ThreadPool threadPool,
                                        ClusterService clusterService, TransportService transportService,
                                        Injector injector) {
        super(settings, GetRiverStateAction.NAME, transportService, clusterService, threadPool);
        this.injector = injector;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.MANAGEMENT;
    }

    @Override
    protected GetRiverStateRequest newRequest() {
        return new GetRiverStateRequest();
    }

    @Override
    protected GetRiverStateResponse newResponse() {
        return new GetRiverStateResponse();
    }

    @Override
    protected void masterOperation(GetRiverStateRequest request, ClusterState state, ActionListener<GetRiverStateResponse> listener) throws ElasticsearchException {
        MetaData metaData = state.metaData();
        RiverStatesMetaData riverStates = metaData.custom(RiverStatesMetaData.TYPE);
        if (request.getRiverName() == null || request.getRiverType() == null) {
            listener.onResponse(new GetRiverStateResponse(request, riverStates.rivers()));
        } else {
            String riverName = request.getRiverName();
            String riverType = request.getRiverType();
            ImmutableList.Builder<RiverState> builder = ImmutableList.builder();
            for (Map.Entry<RiverName, River> entry : RiverHelper.rivers(injector).entrySet()) {
                RiverName name = entry.getKey();
                if (("*".equals(riverName) || name.getName().equals(riverName)) && ("*".equals(riverType) || name.getType().equals(riverType)) && entry.getValue() instanceof StatefulRiver) {
                    StatefulRiver river = (StatefulRiver) entry.getValue();
                    builder.add(river.getRiverState());
                }
            }
            listener.onResponse(new GetRiverStateResponse(request, builder.build()));
        }
    }

}
