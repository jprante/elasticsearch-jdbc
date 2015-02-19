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
package org.xbib.elasticsearch.rest.action.jdbc;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.support.RestToXContentListener;
import org.xbib.elasticsearch.action.jdbc.execute.ExecuteTaskAction;
import org.xbib.elasticsearch.action.jdbc.execute.ExecuteTaskRequest;
import org.xbib.elasticsearch.action.jdbc.execute.ExecuteTaskResponse;

import java.io.IOException;

public class RestRunAction extends BaseRestHandler {

    @Inject
    public RestRunAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);

        controller.registerHandler(Method.POST, "/_jdbc/{name}/_run", this);
    }

    @Override
    protected void handleRequest(RestRequest request, RestChannel channel, Client client) throws Exception {
        try {
            String name = request.param("name");
            ExecuteTaskRequest executeTaskRequest = new ExecuteTaskRequest();
            executeTaskRequest.setName(name);
            client.admin().cluster().execute(ExecuteTaskAction.INSTANCE, executeTaskRequest,
                    new RestToXContentListener<ExecuteTaskResponse>(channel));
        } catch (Throwable t) {
            try {
                channel.sendResponse(new BytesRestResponse(channel, t));
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
                channel.sendResponse(new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR));
            }
        }
    }
}