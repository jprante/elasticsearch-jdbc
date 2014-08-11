package org.xbib.elasticsearch.action.river.jdbc.execute;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.support.nodes.TransportNodesOperationAction;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.service.NodeService;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.xbib.elasticsearch.support.river.RiverHelper;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReferenceArray;

public class TransportRiverExecuteAction extends TransportNodesOperationAction<RiverExecuteRequest, RiverExecuteResponse, NodeRiverExecuteRequest, NodeRiverExecuteResponse> {

    private final NodeService nodeService;

    private final Injector injector;

    @Inject
    public TransportRiverExecuteAction(Settings settings, ClusterName clusterName, ThreadPool threadPool,
                                       ClusterService clusterService, TransportService transportService,
                                       NodeService nodeService, Injector injector) {
        super(settings, clusterName, threadPool, clusterService, transportService);
        this.nodeService = nodeService;
        this.injector = injector;
    }

    @Override
    public String transportAction() {
        return RiverExecuteAction.NAME;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.GENERIC;
    }

    @Override
    protected NodeRiverExecuteResponse nodeOperation(NodeRiverExecuteRequest request) throws ElasticsearchException {
        NodeInfo nodeInfo = nodeService.info(false, true, false, true, false, false, true, false, true);
        String riverType = request.getRiverType();
        String riverName = request.getRiverName();
        for (Map.Entry<RiverName, River> entry : RiverHelper.rivers(injector).entrySet()) {
            RiverName name = entry.getKey();
            if ((riverName == null || name.getName().equals(riverName))
                    && (riverType == null || name.getType().equals(riverType))
                    && entry.getValue() instanceof RunnableRiver) {
                RunnableRiver river = (RunnableRiver) entry.getValue();
                river.run();
                return new NodeRiverExecuteResponse(nodeInfo.getNode()).setExecuted(true);
            }
        }
        return new NodeRiverExecuteResponse(nodeInfo.getNode()).setExecuted(false);
    }

    @Override
    protected RiverExecuteRequest newRequest() {
        return new RiverExecuteRequest();
    }

    @Override
    protected RiverExecuteResponse newResponse(RiverExecuteRequest request, AtomicReferenceArray nodesResponses) {
        boolean[] b = new boolean[nodesResponses.length()];
        for (int i = 0; i < nodesResponses.length(); i++) {
            Object nodesResponse = nodesResponses.get(i);
            if (nodesResponse instanceof NodeRiverExecuteResponse) {
                NodeRiverExecuteResponse nodeRiverExecuteResponse = (NodeRiverExecuteResponse) nodesResponse;
                b[i] = nodeRiverExecuteResponse.isExecuted();
            }
        }
        return new RiverExecuteResponse().setExecuted(b);
    }

    @Override
    protected NodeRiverExecuteRequest newNodeRequest() {
        return new NodeRiverExecuteRequest();
    }

    @Override
    protected NodeRiverExecuteRequest newNodeRequest(String nodeId, RiverExecuteRequest request) {
        return new NodeRiverExecuteRequest(nodeId, request);
    }

    @Override
    protected NodeRiverExecuteResponse newNodeResponse() {
        return new NodeRiverExecuteResponse();
    }

    @Override
    protected boolean accumulateExceptions() {
        return false;
    }

}
