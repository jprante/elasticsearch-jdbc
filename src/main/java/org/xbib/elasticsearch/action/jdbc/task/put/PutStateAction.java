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
package org.xbib.elasticsearch.action.jdbc.task.put;

import org.elasticsearch.action.admin.cluster.ClusterAction;
import org.elasticsearch.client.ClusterAdminClient;

public class PutStateAction extends ClusterAction<PutStateRequest, PutStateResponse, PutStateRequestBuilder> {

    public static final PutStateAction INSTANCE = new PutStateAction();

    public static final String NAME = "org.xbib.elasticsearch.action.jdbc.state.put";

    private PutStateAction() {
        super(NAME);
    }

    @Override
    public PutStateRequestBuilder newRequestBuilder(ClusterAdminClient client) {
        return new PutStateRequestBuilder(client);
    }

    @Override
    public PutStateResponse newResponse() {
        return new PutStateResponse();
    }
}