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
package org.elasticsearch.rest.action;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.XContentRestResponse;
import org.elasticsearch.rest.XContentThrowableRestResponse;
import org.elasticsearch.rest.action.support.RestXContentBuilder;
import org.elasticsearch.river.jdbc.JDBCRiver;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestStatus.OK;

/**
 * The JDBC River REST fire move. The river can be fired once to run
 * when this move is called from REST.
 *
 * Example:
 *
 * curl -XPOST 'localhost:9200/my_jdbc_river/_induce'
 *
 * @author Jörg Prante <joergprante@gmail.com>
 */
public class RestJDBCRiverInduceAction extends BaseRestHandler {

    private final PluginsService pluginsService;

    @Inject
    public RestJDBCRiverInduceAction(Settings settings, Client client,
                                     RestController controller, Injector injector) {
        super(settings, client);
        this.pluginsService = injector.getInstance(PluginsService.class);
        controller.registerHandler(POST, "/{river}/_induce", this);
    }

    @Override
    public void handleRequest(final RestRequest request, RestChannel channel) {
        String riverName = request.param("river");
        ImmutableMap<String, Plugin> plugins = pluginsService.plugins();
        boolean ok = false;
        for (Map.Entry<String, Plugin> me : plugins.entrySet()) {
            if (JDBCRiver.NAME.equals(me.getKey())) {
                JDBCRiver jdbcRiver = (JDBCRiver) me.getValue();
                if (jdbcRiver.riverName().name().equals(riverName)) {
                    jdbcRiver.induce();
                    ok = true;
                }
            }
        }
        try {
            XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
            builder.startObject();
            builder.field("ok", ok);
            builder.endObject();
            channel.sendResponse(new XContentRestResponse(request, OK, builder));
        } catch (IOException e) {
            try {
                channel.sendResponse(new XContentThrowableRestResponse(request, e));
            } catch (IOException e1) {
                logger.error("Failed to send failure response", e1);
            }
        }
    }
}
