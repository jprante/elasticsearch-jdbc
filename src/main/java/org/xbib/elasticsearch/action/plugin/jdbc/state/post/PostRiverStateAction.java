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

import org.elasticsearch.action.admin.cluster.ClusterAction;
import org.elasticsearch.client.ClusterAdminClient;

public class PostRiverStateAction extends ClusterAction<PostRiverStateRequest, PostRiverStateResponse, PostRiverStateRequestBuilder> {

    public static final PostRiverStateAction INSTANCE = new PostRiverStateAction();

    public static final String NAME = "org.xbib.elasticsearch.action.river.jdbc.state.post";

    private PostRiverStateAction() {
        super(NAME);
    }

    @Override
    public PostRiverStateRequestBuilder newRequestBuilder(ClusterAdminClient client) {
        return new PostRiverStateRequestBuilder(client);
    }

    @Override
    public PostRiverStateResponse newResponse() {
        return new PostRiverStateResponse();
    }
}