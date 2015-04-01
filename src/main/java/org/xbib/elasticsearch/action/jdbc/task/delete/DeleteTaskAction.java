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
package org.xbib.elasticsearch.action.jdbc.task.delete;

import org.elasticsearch.action.admin.cluster.ClusterAction;
import org.elasticsearch.client.ClusterAdminClient;

public class DeleteTaskAction extends ClusterAction<DeleteTaskRequest, DeleteTaskResponse, DeleteTaskRequestBuilder> {

    public static final DeleteTaskAction INSTANCE = new DeleteTaskAction();

    public static final String NAME = "org.xbib.elasticsearch.action.task.delete";

    private DeleteTaskAction() {
        super(NAME);
    }

    @Override
    public DeleteTaskRequestBuilder newRequestBuilder(ClusterAdminClient client) {
        return new DeleteTaskRequestBuilder(client);
    }

    @Override
    public DeleteTaskResponse newResponse() {
        return new DeleteTaskResponse();
    }
}