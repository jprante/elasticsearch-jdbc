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
package org.xbib.elasticsearch.action.jdbc.state.post;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.xbib.elasticsearch.jdbc.state.State;
import org.xbib.elasticsearch.jdbc.state.cluster.StateMetaData;
import org.xbib.elasticsearch.jdbc.state.cluster.StateService;

public class TransportPostStateAction extends TransportMasterNodeOperationAction<PostStateRequest, PostStateResponse> {

    private final Injector injector;

    @Inject
    public TransportPostStateAction(Settings settings, ThreadPool threadPool,
                                    ClusterService clusterService, TransportService transportService,
                                    ActionFilters actionFilters,
                                    Injector injector) {
        super(settings, PostStateAction.NAME, transportService, clusterService, threadPool, actionFilters);
        this.injector = injector;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.MANAGEMENT;
    }

    @Override
    protected PostStateRequest newRequest() {
        return new PostStateRequest();
    }

    @Override
    protected PostStateResponse newResponse() {
        return new PostStateResponse();
    }

    @Override
    protected ClusterBlockException checkBlock(PostStateRequest request, ClusterState state) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.METADATA, "");
    }

    @Override
    protected void masterOperation(PostStateRequest request, ClusterState clusterState,
                                   final ActionListener<PostStateResponse> listener)
            throws ElasticsearchException {
        StateMetaData stateMetaData = clusterState.metaData().custom(StateMetaData.TYPE);

        StateService stateService = injector.getInstance(StateService.class);
        ImmutableList<State> states = stateMetaData.getStates(request.getName());
        State state = request.getState();
        if (states.isEmpty()) {
            if (state == null) {
                state = new State();
            }
            state.setName(request.getName());
            state.getMap().put("aborted", request.isAbort());
            state.getMap().put("suspended", request.isSuspend());
        } else {
            // merge old and new, overwrite previous values only if set in request
            state = states.get(0);
            if (request.getState().getStarted() != null) {
                state.setStarted(request.getState().getStarted());
            }
            if (request.getState().getLastActiveBegin() != null) {
                state.setLastActive(request.getState().getLastActiveBegin(), request.getState().getLastActiveEnd());
            }
            if (request.getState().getCounter() != null) {
                state.setCounter(request.getState().getCounter());
            }
            if (request.getState().getMap() != null && !request.getState().getMap().isEmpty()) {
                state.getMap().putAll(request.getState().getMap());
            }
            state.getMap().put("aborted", request.isAbort());
            state.getMap().put("suspended", request.isSuspend());
        }
        stateService.postState(new StateService.StateRequest("post_state[" + request.getName() + "]", state)
                .masterNodeTimeout(request.masterNodeTimeout())
                .ackTimeout(request.ackTimeout()), new ActionListener<ClusterStateUpdateResponse>() {
            @Override
            public void onResponse(ClusterStateUpdateResponse clusterStateUpdateResponse) {
                listener.onResponse(new PostStateResponse(clusterStateUpdateResponse.isAcknowledged()));
            }

            @Override
            public void onFailure(Throwable e) {
                listener.onFailure(e);
            }
        });
    }

}