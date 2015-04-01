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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedRequestBuilder;
import org.elasticsearch.client.ClusterAdminClient;
import org.xbib.elasticsearch.common.task.Task;

public class PutTaskRequestBuilder extends AcknowledgedRequestBuilder<PutTaskRequest, PutTaskResponse, PutTaskRequestBuilder, ClusterAdminClient> {

    public PutTaskRequestBuilder(ClusterAdminClient client) {
        super(client, new PutTaskRequest());
    }

    public PutTaskRequestBuilder setName(String name) {
        request.setName(name);
        return this;
    }

    public PutTaskRequestBuilder setTask(Task task) {
        request.setTask(task);
        return this;
    }

    @Override
    protected void doExecute(ActionListener<PutTaskResponse> listener) {
        client.execute(PutTaskAction.INSTANCE, request, listener);
    }
}