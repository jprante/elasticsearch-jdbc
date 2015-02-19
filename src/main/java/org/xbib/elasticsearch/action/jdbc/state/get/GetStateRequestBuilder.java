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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.MasterNodeReadOperationRequestBuilder;
import org.elasticsearch.client.ClusterAdminClient;

public class GetStateRequestBuilder extends MasterNodeReadOperationRequestBuilder<GetStateRequest, GetStateResponse, GetStateRequestBuilder, ClusterAdminClient> {

    public GetStateRequestBuilder(ClusterAdminClient client) {
        super(client, new GetStateRequest());
    }

    public GetStateRequestBuilder setName(String name) {
        request.setName(name);
        return this;
    }

    @Override
    protected void doExecute(ActionListener<GetStateResponse> listener) {
        client.execute(GetStateAction.INSTANCE, request, listener);
    }
}