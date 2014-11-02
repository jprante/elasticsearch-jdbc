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
package org.xbib.elasticsearch.rest.action.river.jdbc;

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
import org.xbib.elasticsearch.action.plugin.jdbc.run.RunRiverAction;
import org.xbib.elasticsearch.action.plugin.jdbc.run.RunRiverRequest;
import org.xbib.elasticsearch.action.plugin.jdbc.run.RunRiverResponse;

import java.io.IOException;

public class RestRunRiverAction extends BaseRestHandler {

    @Inject
    public RestRunRiverAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);

        controller.registerHandler(Method.POST, "/_river/jdbc/{rivername}/_run", this);
    }

    @Override
    protected void handleRequest(RestRequest request, RestChannel channel, Client client) throws Exception {
        try {
            String riverName = request.param("rivername");
            String riverType = "jdbc";
            RunRiverRequest runRiverRequest = new RunRiverRequest();
            runRiverRequest.setRiverName(riverName).setRiverType(riverType);
            client.admin().cluster().execute(RunRiverAction.INSTANCE, runRiverRequest,
                    new RestToXContentListener<RunRiverResponse>(channel));
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