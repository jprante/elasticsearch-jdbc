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
package org.xbib.elasticsearch.action.jdbc.state.get;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeReadOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.xbib.elasticsearch.jdbc.state.cluster.StateMetaData;

public class TransportGetStateAction extends TransportMasterNodeReadOperationAction<GetStateRequest, GetStateResponse> {

    @Inject
    public TransportGetStateAction(Settings settings, ThreadPool threadPool,
                                   ClusterService clusterService, TransportService transportService,
                                   ActionFilters actionFilters) {
        super(settings, GetStateAction.NAME, transportService, clusterService, threadPool, actionFilters);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected GetStateRequest newRequest() {
        return new GetStateRequest();
    }

    @Override
    protected GetStateResponse newResponse() {
        return new GetStateResponse();
    }

    @Override
    protected void masterOperation(final GetStateRequest request,
                                   final ClusterState clusterState,
                                   final ActionListener<GetStateResponse> listener)
            throws ElasticsearchException {
        StateMetaData stateMetaData = clusterState.metaData().custom(StateMetaData.TYPE);
        listener.onResponse(new GetStateResponse(stateMetaData != null ?
                stateMetaData.getStates(request.getName()) : null));
    }

    @Override
    protected ClusterBlockException checkBlock(GetStateRequest request, ClusterState state) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.METADATA, "");
    }

}