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
package org.xbib.elasticsearch.action.jdbc.task.post;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.NodeOperationRequest;
import org.elasticsearch.action.support.nodes.TransportNodesOperationAction;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.xbib.elasticsearch.common.task.Task;
import org.xbib.elasticsearch.common.task.cluster.ClusterTaskMetaData;
import org.xbib.elasticsearch.common.task.cluster.ClusterTaskService;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static org.elasticsearch.common.collect.Lists.newArrayList;

public class TransportPostStateAction extends TransportNodesOperationAction<PostTaskRequest, PostTaskResponse, TransportPostStateAction.NodeTaskRequest, PostTaskResponse.NodeTaskResponse> {

    private final Node node;

    private final Injector injector;

    @Inject
    public TransportPostStateAction(Settings settings, ClusterName clusterName, ThreadPool threadPool,
                                    ClusterService clusterService, TransportService transportService,
                                    Node node, ActionFilters actionFilters,
                                    Injector injector) {
        super(settings, PostTaskAction.NAME, clusterName, threadPool, clusterService, transportService, actionFilters);
        this.node = node;
        this.injector = injector;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.GENERIC;
    }

    @Override
    protected PostTaskRequest newRequest() {
        return new PostTaskRequest();
    }

    @Override
    protected PostTaskResponse newResponse(PostTaskRequest request, AtomicReferenceArray responses) {
        final List<PostTaskResponse.NodeTaskResponse> nodeResponses = newArrayList();
        for (int i = 0; i < responses.length(); i++) {
            Object resp = responses.get(i);
            if (resp instanceof PostTaskResponse.NodeTaskResponse) {
                nodeResponses.add((PostTaskResponse.NodeTaskResponse) resp);
            }
        }
        return new PostTaskResponse(clusterName, nodeResponses.toArray(new PostTaskResponse.NodeTaskResponse[nodeResponses.size()]));
    }

    @Override
    protected NodeTaskRequest newNodeRequest() {
        return new NodeTaskRequest();
    }

    @Override
    protected NodeTaskRequest newNodeRequest(String clusterName, PostTaskRequest request) {
        return new NodeTaskRequest(clusterName, request);
    }

    @Override
    protected PostTaskResponse.NodeTaskResponse newNodeResponse() {
        return new PostTaskResponse.NodeTaskResponse();
    }

    @Override
    protected PostTaskResponse.NodeTaskResponse nodeOperation(NodeTaskRequest nodeTaskRequest) throws ElasticsearchException {
        ClusterTaskMetaData metaData = clusterService.state().metaData().custom(ClusterTaskMetaData.TYPE);
        ClusterTaskService service = injector.getInstance(ClusterTaskService.class);
        PostTaskRequest request = nodeTaskRequest.getRequest();
        ImmutableList<Task> tasks = metaData.getTask(request.getName());
        Task task = request.getTask();
        if (tasks.isEmpty()) {
            if (task == null) {
                task = new Task();
            }
            task.setName(request.getName());
            task.getMap().put("aborted", request.isAbort());
            task.getMap().put("suspended", request.isSuspend());
        } else {
            // merge old and new, overwrite previous values only if set in request
            task = tasks.get(0);
            if (request.getTask().getStarted() != null) {
                task.setStarted(request.getTask().getStarted());
            }
            if (request.getTask().getLastActiveBegin() != null) {
                task.setLastActive(request.getTask().getLastActiveBegin(), request.getTask().getLastActiveEnd());
            }
            if (request.getTask().getMap() != null && !request.getTask().getMap().isEmpty()) {
                task.getMap().putAll(request.getTask().getMap());
            }
            task.getMap().put("aborted", request.isAbort());
            task.getMap().put("suspended", request.isSuspend());
        }
        service.postTask(new ClusterTaskService.TaskRequest("post_task[" + request.getName() + "]", task)
               , new ActionListener<ClusterStateUpdateResponse>() {
            @Override
            public void onResponse(ClusterStateUpdateResponse clusterStateUpdateResponse) {
                //listener.onResponse(new PostTaskResponse());
                logger.info("success of cluster state update");
            }

            @Override
            public void onFailure(Throwable e) {
                //listener.onFailure(e);
            }
        });
        return new PostTaskResponse.NodeTaskResponse(clusterService.localNode());
    }

    @Override
    protected boolean accumulateExceptions() {
        return false;
    }

    protected static class NodeTaskRequest extends NodeOperationRequest {

        PostTaskRequest request;

        private NodeTaskRequest() {
        }

        private NodeTaskRequest(String nodeId, PostTaskRequest request) {
            super(request, nodeId);
            this.request = request;
        }

        public PostTaskRequest getRequest() {
            return request;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
        }
    }

}