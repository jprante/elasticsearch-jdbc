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
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.support.RestToXContentListener;
import org.xbib.elasticsearch.action.plugin.jdbc.state.delete.DeleteRiverStateAction;
import org.xbib.elasticsearch.action.plugin.jdbc.state.delete.DeleteRiverStateRequest;
import org.xbib.elasticsearch.action.plugin.jdbc.state.delete.DeleteRiverStateResponse;
import org.xbib.elasticsearch.action.plugin.jdbc.state.get.GetRiverStateAction;
import org.xbib.elasticsearch.action.plugin.jdbc.state.get.GetRiverStateRequest;
import org.xbib.elasticsearch.action.plugin.jdbc.state.get.GetRiverStateResponse;
import org.xbib.elasticsearch.action.plugin.jdbc.state.post.PostRiverStateAction;
import org.xbib.elasticsearch.action.plugin.jdbc.state.post.PostRiverStateRequest;
import org.xbib.elasticsearch.action.plugin.jdbc.state.post.PostRiverStateResponse;
import org.xbib.elasticsearch.plugin.jdbc.state.RiverState;

import java.io.IOException;

public class RestRiverStateAction extends BaseRestHandler {

    private final Client client;

    @Inject
    public RestRiverStateAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        this.client = client;

        controller.registerHandler(RestRequest.Method.GET,
                "/_river/jdbc/{rivername}/_state", new Get());
        controller.registerHandler(RestRequest.Method.POST,
                "/_river/jdbc/{rivername}/_state", new Post(false, false, false));
        controller.registerHandler(RestRequest.Method.DELETE,
                "/_river/jdbc/{rivername}/_state", new Delete());

        controller.registerHandler(RestRequest.Method.POST,
                "/_river/jdbc/{rivername}/_abort", new Post(true, false, false));
        controller.registerHandler(RestRequest.Method.POST,
                "/_river/jdbc/{rivername}/_suspend", new Post(false, true, false));
        controller.registerHandler(RestRequest.Method.POST,
                "/_river/jdbc/{rivername}/_resume", new Post(false, false, true));
    }

    @Override
    protected void handleRequest(RestRequest request, RestChannel channel, Client client) throws Exception {
        channel.sendResponse(new BytesRestResponse(RestStatus.NOT_IMPLEMENTED));
    }

    class Get implements RestHandler {

        @Override
        public void handleRequest(RestRequest request, RestChannel channel) throws Exception {
            try {
                String riverName = request.param("rivername");
                String riverType = "jdbc";
                GetRiverStateRequest riverStateRequest = new GetRiverStateRequest();
                riverStateRequest.setRiverName(riverName).setRiverType(riverType);
                client.admin().cluster().execute(GetRiverStateAction.INSTANCE, riverStateRequest,
                        new RestToXContentListener<GetRiverStateResponse>(channel));
            } catch (Throwable t) {
                logger.error(t.getMessage(), t);
                try {
                    channel.sendResponse(new BytesRestResponse(channel, t));
                } catch (IOException e) {
                    logger.error(e.getMessage(), e);
                    channel.sendResponse(new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR));
                }
            }
        }
    }

    class Post implements RestHandler {

        boolean abort;
        boolean suspend;
        boolean resume;

        Post(boolean abort, boolean suspend, boolean resume) {
            this.abort = abort;
            this.resume = resume;
            this.suspend = suspend;
        }

        @Override
        public void handleRequest(RestRequest request, RestChannel channel) throws Exception {
            try {
                String riverName = request.param("rivername");
                String riverType = "jdbc";
                PostRiverStateRequest postRiverStateRequest = new PostRiverStateRequest();
                postRiverStateRequest.setRiverName(riverName).setRiverType(riverType);
                if (request.hasContent()) {
                    RiverState riverState = new RiverState().setName(riverName).setType(riverType);
                    riverState.setMap(XContentHelper.convertToMap(request.content(), true).v2());
                    postRiverStateRequest.setRiverState(riverState);
                }
                if (abort) {
                    postRiverStateRequest.setAbort();
                }
                if (suspend) {
                    postRiverStateRequest.setSuspend();
                }
                if (resume) {
                    postRiverStateRequest.setResume();
                }
                client.admin().cluster().execute(PostRiverStateAction.INSTANCE, postRiverStateRequest,
                        new RestToXContentListener<PostRiverStateResponse>(channel));
            } catch (Throwable t) {
                logger.error(t.getMessage(), t);
                try {
                    channel.sendResponse(new BytesRestResponse(channel, t));
                } catch (IOException e) {
                    logger.error(e.getMessage(), e);
                    channel.sendResponse(new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR));
                }
            }
        }
    }

    class Delete implements RestHandler {

        @Override
        public void handleRequest(RestRequest request, RestChannel channel) throws Exception {
            try {
                String riverName = request.param("rivername");
                String riverType = "jdbc";
                DeleteRiverStateRequest riverStateRequest = new DeleteRiverStateRequest();
                riverStateRequest.setRiverName(riverName).setRiverType(riverType);
                client.admin().cluster().execute(DeleteRiverStateAction.INSTANCE, riverStateRequest,
                        new RestToXContentListener<DeleteRiverStateResponse>(channel));
            } catch (Throwable t) {
                logger.error(t.getMessage(), t);
                try {
                    channel.sendResponse(new BytesRestResponse(channel, t));
                } catch (IOException e) {
                    logger.error(e.getMessage(), e);
                    channel.sendResponse(new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR));
                }
            }
        }
    }
}
