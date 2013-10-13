/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.xbib.elasticsearch.rest.action;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.XContentRestResponse;
import org.elasticsearch.rest.XContentThrowableRestResponse;
import org.elasticsearch.rest.action.support.RestXContentBuilder;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiversService;
import org.xbib.elasticsearch.river.jdbc.JDBCRiver;
import org.xbib.elasticsearch.river.jdbc.JDBCRiver;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Map;

/**
 * The JDBC River REST fire move. The river can be fired once to run
 * when this move is called from REST.
 * <p>
 * Example:<br/>
 * <code>
 * curl -XPOST 'localhost:9200/_river/my_jdbc_river/_induce'
 * </code>
 * @author Jörg Prante <joergprante@gmail.com>
 * @author pdegeus
 */
public class RestJDBCRiverInduceAction extends BaseRestHandler {

    private final RiversService riversService;

    @Inject
    public RestJDBCRiverInduceAction(Settings settings, Client client,
                                     RestController controller, Injector injector) {
        super(settings, client);
        this.riversService = injector.getInstance(RiversService.class);
        controller.registerHandler(RestRequest.Method.POST, "/_river/{river}/_induce", this);
    }

    @Override
    public void handleRequest(final RestRequest request, RestChannel channel) {

        //Get and check river name parameter
        String riverName = request.param("river");
        if (riverName == null || riverName.isEmpty()) {
            respond(false, request, channel, "Parameter 'river' is required", RestStatus.BAD_REQUEST);
            return;
        }

        //Retrieve the registered rivers using reflection (UGLY HACK!!)
        //TODO: Obtain the rivers using public API
        ImmutableMap<RiverName, River> rivers;
        try {
            Field field = RiversService.class.getDeclaredField("rivers");
            field.setAccessible(true);
            rivers = (ImmutableMap<RiverName, River>) field.get(riversService);
        } catch (NoSuchFieldException e) {
            errorResponse(request, channel, e);
            return;
        } catch (IllegalAccessException e) {
            errorResponse(request, channel, e);
            return;
        }

        boolean found = false;
        for (Map.Entry<RiverName, River> entry : rivers.entrySet()) {
            RiverName name = entry.getKey();

            if (name.getName().equals(riverName)) {
                if (!name.getType().equals(JDBCRiver.TYPE)) {
                    respond(
                        false, request, channel,
                        "River '" + riverName + "' is not a jdbc-river, but has type " + name.getType(),
                        RestStatus.UNPROCESSABLE_ENTITY
                    );
                    return;
                }

                JDBCRiver jdbcRiver = (JDBCRiver) entry.getValue();
                jdbcRiver.induce();
                found = true;
                break;
            }
        }

        String error = found ? null : "River not found: " + riverName;
        respond(found, request, channel, error, RestStatus.OK);
    }

    private void respond(boolean success, RestRequest request, RestChannel channel, String error, RestStatus status) {
        try {
            XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
            builder.startObject();
            builder.field("success", success);
            if (error != null) {
                builder.field("error", error);
            }
            builder.endObject();
            channel.sendResponse(new XContentRestResponse(request, status, builder));
        } catch (IOException e) {
            errorResponse(request, channel, e);
        }
    }

    private void errorResponse(RestRequest request, RestChannel channel, Throwable e) {
        try {
            channel.sendResponse(new XContentThrowableRestResponse(request, e));
        } catch (IOException e1) {
            logger.error("Failed to send failure response", e1);
        }
    }

}
