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
package org.xbib.elasticsearch.action.plugin.jdbc.state.put;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedRequestBuilder;
import org.elasticsearch.client.ClusterAdminClient;
import org.xbib.elasticsearch.plugin.jdbc.state.RiverState;

public class PutRiverStateRequestBuilder extends AcknowledgedRequestBuilder<PutRiverStateRequest, PutRiverStateResponse, PutRiverStateRequestBuilder, ClusterAdminClient> {

    public PutRiverStateRequestBuilder(ClusterAdminClient client) {
        super(client, new PutRiverStateRequest());
    }

    public PutRiverStateRequestBuilder setRiverName(String riverName) {
        request.setRiverName(riverName);
        return this;
    }

    public PutRiverStateRequestBuilder setRiverType(String riverType) {
        request.setRiverType(riverType);
        return this;
    }

    public PutRiverStateRequestBuilder setRiverState(RiverState riverState) {
        request.setRiverState(riverState);
        return this;
    }

    @Override
    protected void doExecute(ActionListener<PutRiverStateResponse> listener) {
        client.execute(PutRiverStateAction.INSTANCE, request, listener);
    }
}