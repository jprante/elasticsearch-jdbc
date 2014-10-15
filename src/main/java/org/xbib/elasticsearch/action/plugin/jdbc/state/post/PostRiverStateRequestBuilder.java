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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedRequestBuilder;
import org.elasticsearch.client.ClusterAdminClient;
import org.xbib.elasticsearch.plugin.jdbc.state.RiverState;

public class PostRiverStateRequestBuilder extends AcknowledgedRequestBuilder<PostRiverStateRequest, PostRiverStateResponse, PostRiverStateRequestBuilder, ClusterAdminClient> {

    public PostRiverStateRequestBuilder(ClusterAdminClient client) {
        super(client, new PostRiverStateRequest());
    }

    public PostRiverStateRequestBuilder setRiverName(String riverName) {
        request.setRiverName(riverName);
        return this;
    }

    public PostRiverStateRequestBuilder setRiverType(String riverType) {
        request.setRiverType(riverType);
        return this;
    }

    public PostRiverStateRequestBuilder setRiverState(RiverState riverState) {
        request.setRiverState(riverState);
        return this;
    }

    public PostRiverStateRequestBuilder setAbort() {
        request.setAbort();
        return this;
    }

    public PostRiverStateRequestBuilder setSuspend() {
        request.setSuspend();
        return this;
    }

    public PostRiverStateRequestBuilder setResume() {
        request.setResume();
        return this;
    }

    @Override
    protected void doExecute(ActionListener<PostRiverStateResponse> listener) {
        client.execute(PostRiverStateAction.INSTANCE, request, listener);
    }
}