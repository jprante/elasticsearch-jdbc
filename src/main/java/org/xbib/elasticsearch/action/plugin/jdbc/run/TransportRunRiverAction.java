/*
 * Copyright (C) 2014 Jörg Prante
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.xbib.elasticsearch.action.plugin.jdbc.run;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.NodeOperationRequest;
import org.elasticsearch.action.support.nodes.NodeOperationResponse;
import org.elasticsearch.action.support.nodes.TransportNodesOperationAction;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.service.NodeService;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiversService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.xbib.elasticsearch.plugin.jdbc.execute.RunnableRiver;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReferenceArray;

public class TransportRunRiverAction extends TransportNodesOperationAction<RunRiverRequest, RunRiverResponse, TransportRunRiverAction.NodeRiverExecuteRequest, TransportRunRiverAction.NodeRiverExecuteResponse> {

    private final Injector injector;

    @Inject
    public TransportRunRiverAction(Settings settings, ClusterName clusterName, ThreadPool threadPool,
                                   ClusterService clusterService, TransportService transportService,
                                   ActionFilters actionFilters,
                                   Injector injector) {
        super(settings, RunRiverAction.NAME, clusterName, threadPool, clusterService, transportService, actionFilters);
        this.injector = injector;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.GENERIC;
    }

    @Override
    protected NodeRiverExecuteResponse nodeOperation(NodeRiverExecuteRequest request) throws ElasticsearchException {
        RiversService riversService = injector.getInstance(RiversService.class);
        NodeService nodeService = injector.getInstance(NodeService.class);
        NodeInfo nodeInfo = nodeService.info(false, true, false, true, false, false, true, false, true);
        for (Map.Entry<RiverName, River> entry : rivers(riversService).entrySet()) {
            RiverName name = entry.getKey();
            if ((request.getRiverName() == null || name.getName().equals(request.getRiverName()))
                    && (request.getRiverType() == null || name.getType().equals(request.getRiverType()))
                    && entry.getValue() instanceof RunnableRiver) {
                RunnableRiver river = (RunnableRiver) entry.getValue();
                river.run();
                return new NodeRiverExecuteResponse(nodeInfo.getNode()).setExecuted(true);
            }
        }
        return new NodeRiverExecuteResponse(nodeInfo.getNode()).setExecuted(false);
    }

    @Override
    protected RunRiverRequest newRequest() {
        return new RunRiverRequest();
    }

    @Override
    protected RunRiverResponse newResponse(RunRiverRequest request, AtomicReferenceArray nodesResponses) {
        boolean[] b = new boolean[nodesResponses.length()];
        for (int i = 0; i < nodesResponses.length(); i++) {
            Object nodesResponse = nodesResponses.get(i);
            if (nodesResponse instanceof NodeRiverExecuteResponse) {
                NodeRiverExecuteResponse nodeRiverExecuteResponse = (NodeRiverExecuteResponse) nodesResponse;
                b[i] = nodeRiverExecuteResponse.isExecuted();
            }
        }
        return new RunRiverResponse().setExecuted(b);
    }

    @Override
    protected NodeRiverExecuteRequest newNodeRequest() {
        return new NodeRiverExecuteRequest();
    }

    @Override
    protected NodeRiverExecuteRequest newNodeRequest(String nodeId, RunRiverRequest request) {
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

    @SuppressWarnings({"unchecked"})
    public static ImmutableMap<RiverName, River> rivers(RiversService riversService) {
        try {
            Field field = RiversService.class.getDeclaredField("rivers");
            if (field != null) {
                field.setAccessible(true);
                return (ImmutableMap<RiverName, River>) field.get(riversService);
            }
        } catch (Throwable e) {
            // ignore
        }
        // if error, do not return anything
        return ImmutableMap.of();
    }

    class NodeRiverExecuteRequest extends NodeOperationRequest {

        private String riverType;

        private String riverName;

        NodeRiverExecuteRequest() {
        }

        public NodeRiverExecuteRequest(String nodeId, RunRiverRequest request) {
            super(request, nodeId);
            this.riverName = request.getRiverName();
            this.riverType = request.getRiverType();
        }

        public NodeRiverExecuteRequest setRiverType(String riverType) {
            this.riverType = riverType;
            return this;
        }

        public String getRiverType() {
            return riverType;
        }

        public NodeRiverExecuteRequest setRiverName(String riverName) {
            this.riverName = riverName;
            return this;
        }

        public String getRiverName() {
            return riverName;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            this.riverName = in.readString();
            this.riverType = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(riverName);
            out.writeString(riverType);
        }
    }

    class NodeRiverExecuteResponse extends NodeOperationResponse {

        private boolean executed;

        NodeRiverExecuteResponse() {
        }

        public NodeRiverExecuteResponse(DiscoveryNode node) {
            super(node);
        }

        public NodeRiverExecuteResponse setExecuted(boolean b) {
            this.executed = b;
            return this;
        }

        public boolean isExecuted() {
            return executed;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            executed = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeBoolean(executed);
        }

    }
}