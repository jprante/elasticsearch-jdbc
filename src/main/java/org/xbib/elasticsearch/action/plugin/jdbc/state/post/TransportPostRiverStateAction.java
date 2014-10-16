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
package org.xbib.elasticsearch.action.plugin.jdbc.state.post;

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
import org.xbib.elasticsearch.plugin.jdbc.state.RiverState;
import org.xbib.elasticsearch.plugin.jdbc.state.RiverStateService;
import org.xbib.elasticsearch.plugin.jdbc.state.RiverStatesMetaData;

public class TransportPostRiverStateAction extends TransportMasterNodeOperationAction<PostRiverStateRequest, PostRiverStateResponse> {

    private final Injector injector;

    @Inject
    public TransportPostRiverStateAction(Settings settings, ThreadPool threadPool,
                                         ClusterService clusterService, TransportService transportService,
                                         ActionFilters actionFilters,
                                         Injector injector) {
        super(settings, PostRiverStateAction.NAME, transportService, clusterService, threadPool, actionFilters);
        this.injector = injector;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.MANAGEMENT;
    }

    @Override
    protected PostRiverStateRequest newRequest() {
        return new PostRiverStateRequest();
    }

    @Override
    protected PostRiverStateResponse newResponse() {
        return new PostRiverStateResponse();
    }

    @Override
    protected ClusterBlockException checkBlock(PostRiverStateRequest request, ClusterState state) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.METADATA, "");
    }

    @Override
    protected void masterOperation(PostRiverStateRequest request, ClusterState clusterState,
                                   final ActionListener<PostRiverStateResponse> listener)
            throws ElasticsearchException {
        RiverStatesMetaData riverStatesMetaData = clusterState.metaData().custom(RiverStatesMetaData.TYPE);

        RiverStateService riverStateService = injector.getInstance(RiverStateService.class);
        ImmutableList<RiverState> riverStates = riverStatesMetaData.getRiverStates(request.getRiverName(), request.getRiverType());
        RiverState riverState = request.getRiverState();
        if (riverStates.isEmpty()) {
            if (riverState == null) {
                riverState = new RiverState();
            }
            riverState.setName(request.getRiverName());
            riverState.setType(request.getRiverType());
            riverState.getMap().put("aborted", request.isAbort());
            riverState.getMap().put("suspended", request.isSuspend());
        } else {
            // merge old and new, overwrite previous values only if set in request
            riverState = riverStates.get(0);
            if (request.getRiverState().getStarted() != null) {
                riverState.setStarted(request.getRiverState().getStarted());
            }
            if (request.getRiverState().getLastActiveBegin() != null) {
                riverState.setLastActive(request.getRiverState().getLastActiveBegin(), request.getRiverState().getLastActiveEnd());
            }
            if (request.getRiverState().getCounter() != null) {
                riverState.setCounter(request.getRiverState().getCounter());
            }
            if (request.getRiverState().getMap() != null && !request.getRiverState().getMap().isEmpty()) {
                riverState.getMap().putAll(request.getRiverState().getMap());
            }
            riverState.getMap().put("aborted", request.isAbort());
            riverState.getMap().put("suspended", request.isSuspend());
        }
        riverStateService.postRiverState(new RiverStateService.RiverStateRequest("post_river_state[" + request.getRiverName() + "]", riverState)
                .masterNodeTimeout(request.masterNodeTimeout())
                .ackTimeout(request.ackTimeout()), new ActionListener<ClusterStateUpdateResponse>() {
            @Override
            public void onResponse(ClusterStateUpdateResponse clusterStateUpdateResponse) {
                listener.onResponse(new PostRiverStateResponse(clusterStateUpdateResponse.isAcknowledged()));
            }

            @Override
            public void onFailure(Throwable e) {
                listener.onFailure(e);
            }
        });
    }

}