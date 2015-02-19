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
package org.xbib.elasticsearch.action.jdbc.state.put;

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
import org.xbib.elasticsearch.jdbc.state.State;
import org.xbib.elasticsearch.jdbc.state.cluster.StateService;

public class TransportPutStateAction extends TransportMasterNodeOperationAction<PutStateRequest, PutStateResponse> {

    private final Injector injector;

    @Inject
    public TransportPutStateAction(Settings settings, ThreadPool threadPool,
                                   ClusterService clusterService, TransportService transportService,
                                   ActionFilters actionFilters,
                                   Injector injector) {
        super(settings, PutStateAction.NAME, transportService, clusterService, threadPool, actionFilters);
        this.injector = injector;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.MANAGEMENT;
    }

    @Override
    protected PutStateRequest newRequest() {
        return new PutStateRequest();
    }

    @Override
    protected PutStateResponse newResponse() {
        return new PutStateResponse();
    }

    @Override
    protected ClusterBlockException checkBlock(PutStateRequest request, ClusterState state) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.METADATA, "");
    }

    @Override
    protected void masterOperation(PutStateRequest request, ClusterState clusterState, final ActionListener<PutStateResponse> listener) throws ElasticsearchException {
        StateService stateService = injector.getInstance(StateService.class);
        State state = request.getState();
        if (state == null) {
            state = new State();
        }
        state.setName(request.getName());
        stateService.putState(new StateService.StateRequest("put_state[" + request.getName() + "]", state)
                .masterNodeTimeout(request.masterNodeTimeout())
                .ackTimeout(request.ackTimeout()), new ActionListener<ClusterStateUpdateResponse>() {
            @Override
            public void onResponse(ClusterStateUpdateResponse clusterStateUpdateResponse) {
                listener.onResponse(new PutStateResponse(clusterStateUpdateResponse.isAcknowledged()));
            }

            @Override
            public void onFailure(Throwable e) {
                listener.onFailure(e);
            }
        });
    }

}