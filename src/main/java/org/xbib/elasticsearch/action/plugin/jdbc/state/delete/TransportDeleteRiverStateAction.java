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
package org.xbib.elasticsearch.action.plugin.jdbc.state.delete;

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
import org.xbib.elasticsearch.plugin.jdbc.state.RiverStateService;

public class TransportDeleteRiverStateAction extends TransportMasterNodeOperationAction<DeleteRiverStateRequest, DeleteRiverStateResponse> {

    private final Injector injector;

    @Inject
    public TransportDeleteRiverStateAction(Settings settings, ThreadPool threadPool,
                                           ClusterService clusterService, TransportService transportService,
                                           ActionFilters actionFilters,
                                           Injector injector) {
        super(settings, DeleteRiverStateAction.NAME, transportService, clusterService, threadPool, actionFilters);
        this.injector = injector;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.MANAGEMENT;
    }

    @Override
    protected DeleteRiverStateRequest newRequest() {
        return new DeleteRiverStateRequest();
    }

    @Override
    protected DeleteRiverStateResponse newResponse() {
        return new DeleteRiverStateResponse();
    }

    @Override
    protected ClusterBlockException checkBlock(DeleteRiverStateRequest request, ClusterState state) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.METADATA, "");
    }

    @Override
    protected void masterOperation(DeleteRiverStateRequest request, ClusterState state, final ActionListener<DeleteRiverStateResponse> listener) throws ElasticsearchException {
        RiverStateService riverStateService = injector.getInstance(RiverStateService.class);
        riverStateService.deleteRiverState(new RiverStateService.DeleteRiverStateRequest("delete_river_state[" + request.getRiverName() + "/" + request.getRiverType() + "]", request.getRiverName(), request.getRiverType())
                .masterNodeTimeout(request.masterNodeTimeout())
                .ackTimeout(request.ackTimeout()), new ActionListener<ClusterStateUpdateResponse>() {
            @Override
            public void onResponse(ClusterStateUpdateResponse clusterStateUpdateResponse) {
                listener.onResponse(new DeleteRiverStateResponse(clusterStateUpdateResponse.isAcknowledged()));
            }

            @Override
            public void onFailure(Throwable e) {
                listener.onFailure(e);
            }
        });
    }

}