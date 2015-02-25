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
package org.xbib.elasticsearch.action.jdbc.task;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.NodeOperationRequest;
import org.elasticsearch.action.support.nodes.NodeOperationResponse;
import org.elasticsearch.action.support.nodes.TransportNodesOperationAction;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.service.NodeService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReferenceArray;

public class TransportExecuteTaskAction extends TransportNodesOperationAction<ExecuteTaskRequest, ExecuteTaskResponse, TransportExecuteTaskAction.NodeExecuteRequest, TransportExecuteTaskAction.NodeExecuteResponse> {

    private final Injector injector;

    @Inject
    public TransportExecuteTaskAction(Settings settings, ClusterName clusterName, ThreadPool threadPool,
                                      ClusterService clusterService, TransportService transportService,
                                      ActionFilters actionFilters,
                                      Injector injector) {
        super(settings, ExecuteTaskAction.NAME, clusterName, threadPool, clusterService, transportService, actionFilters);
        this.injector = injector;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.GENERIC;
    }

    @Override
    protected NodeExecuteResponse nodeOperation(NodeExecuteRequest request) throws ElasticsearchException {
        NodeService nodeService = injector.getInstance(NodeService.class);
        NodeInfo nodeInfo = nodeService.info(false, true, false, true, false, false, true, false, true);
        /*RiversService riversService = injector.getInstance(RiversService.class);
        for (Map.Entry<RiverName, River> entry : rivers(riversService).entrySet()) {
            RiverName name = entry.getKey();
            if ((request.getRiverName() == null || name.getName().equals(request.getRiverName()))
                    && (request.getRiverType() == null || name.getType().equals(request.getRiverType()))
                    && entry.getValue() instanceof RunnableRiver) {
                RunnableRiver river = (RunnableRiver) entry.getValue();
                river.run();
                return new NodeRiverExecuteResponse(nodeInfo.getNode()).setExecuted(true);
            }
        }*/
        return new NodeExecuteResponse(nodeInfo.getNode()).setExecuted(false);
    }

    @Override
    protected ExecuteTaskRequest newRequest() {
        return new ExecuteTaskRequest();
    }

    @Override
    protected ExecuteTaskResponse newResponse(ExecuteTaskRequest request, AtomicReferenceArray nodesResponses) {
        boolean[] b = new boolean[nodesResponses.length()];
        for (int i = 0; i < nodesResponses.length(); i++) {
            Object nodesResponse = nodesResponses.get(i);
            if (nodesResponse instanceof NodeExecuteResponse) {
                NodeExecuteResponse nodeExecuteResponse = (NodeExecuteResponse) nodesResponse;
                b[i] = nodeExecuteResponse.isExecuted();
            }
        }
        return new ExecuteTaskResponse().setExecuted(b);
    }

    @Override
    protected NodeExecuteRequest newNodeRequest() {
        return new NodeExecuteRequest();
    }

    @Override
    protected NodeExecuteRequest newNodeRequest(String nodeId, ExecuteTaskRequest request) {
        return new NodeExecuteRequest(nodeId, request);
    }

    @Override
    protected NodeExecuteResponse newNodeResponse() {
        return new NodeExecuteResponse();
    }

    @Override
    protected boolean accumulateExceptions() {
        return false;
    }


    class NodeExecuteRequest extends NodeOperationRequest {

        private String name;

        NodeExecuteRequest() {
        }

        public NodeExecuteRequest(String nodeId, ExecuteTaskRequest request) {
            super(request, nodeId);
            this.name = request.getName();
        }

        public NodeExecuteRequest setName(String name) {
            this.name = name;
            return this;
        }

        public String getName() {
            return name;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            this.name = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(name);
        }
    }

    class NodeExecuteResponse extends NodeOperationResponse {

        private boolean executed;

        NodeExecuteResponse() {
        }

        public NodeExecuteResponse(DiscoveryNode node) {
            super(node);
        }

        public NodeExecuteResponse setExecuted(boolean b) {
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