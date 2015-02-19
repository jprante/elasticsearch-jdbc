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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedRequestBuilder;
import org.elasticsearch.client.ClusterAdminClient;
import org.xbib.elasticsearch.jdbc.state.State;

public class PostStateRequestBuilder extends AcknowledgedRequestBuilder<PostStateRequest, PostStateResponse, PostStateRequestBuilder, ClusterAdminClient> {

    public PostStateRequestBuilder(ClusterAdminClient client) {
        super(client, new PostStateRequest());
    }

    public PostStateRequestBuilder setName(String name) {
        request.setName(name);
        return this;
    }

    public PostStateRequestBuilder setState(State state) {
        request.setState(state);
        return this;
    }

    public PostStateRequestBuilder setAbort() {
        request.setAbort();
        return this;
    }

    public PostStateRequestBuilder setSuspend() {
        request.setSuspend();
        return this;
    }

    public PostStateRequestBuilder setResume() {
        request.setResume();
        return this;
    }

    @Override
    protected void doExecute(ActionListener<PostStateResponse> listener) {
        client.execute(PostStateAction.INSTANCE, request, listener);
    }
}