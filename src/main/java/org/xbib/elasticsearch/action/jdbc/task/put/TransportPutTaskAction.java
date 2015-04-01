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
package org.xbib.elasticsearch.action.jdbc.task.put;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.xbib.elasticsearch.common.task.Task;
import org.xbib.elasticsearch.common.task.cluster.ClusterTaskService;

public class TransportPutTaskAction extends TransportMasterNodeOperationAction<PutTaskRequest, PutTaskResponse> {

    private final Injector injector;

    @Inject
    public TransportPutTaskAction(Settings settings, ThreadPool threadPool,
                                  ClusterService clusterService, TransportService transportService,
                                  ActionFilters actionFilters,
                                  Injector injector) {
        super(settings, PutTaskAction.NAME, transportService, clusterService, threadPool, actionFilters);
        this.injector = injector;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.MANAGEMENT;
    }

    @Override
    protected PutTaskRequest newRequest() {
        return new PutTaskRequest();
    }

    @Override
    protected PutTaskResponse newResponse() {
        return new PutTaskResponse();
    }

    @Override
    protected ClusterBlockException checkBlock(PutTaskRequest request, ClusterState state) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.METADATA, "");
    }

    @Override
    protected void masterOperation(PutTaskRequest request, ClusterState clusterState, final ActionListener<PutTaskResponse> listener) throws ElasticsearchException {
        ClusterTaskService service = injector.getInstance(ClusterTaskService.class);
        Task task = request.getTask();
        if (task == null) {
            task = new Task();
        }
        task.setName(request.getName());
        service.putTask(new ClusterTaskService.TaskRequest("put_task[" + request.getName() + "]", task)
                .masterNodeTimeout(request.masterNodeTimeout())
                .ackTimeout(request.ackTimeout()), new ActionListener<ClusterStateUpdateResponse>() {
            @Override
            public void onResponse(ClusterStateUpdateResponse clusterStateUpdateResponse) {
                listener.onResponse(new PutTaskResponse(clusterStateUpdateResponse.isAcknowledged()));
            }

            @Override
            public void onFailure(Throwable e) {
                listener.onFailure(e);
            }
        });
    }

}