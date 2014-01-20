package org.xbib.elasticsearch.rest.action;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.XContentRestResponse;
import org.elasticsearch.rest.XContentThrowableRestResponse;
import org.elasticsearch.rest.action.support.RestXContentBuilder;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;

import org.xbib.elasticsearch.river.jdbc.JDBCRiver;

import java.io.IOException;
import java.util.Map;

public class RestJDBCRiverStateAction extends AbstractRestRiverAction {

    @Inject
    public RestJDBCRiverStateAction(Settings settings, Client client,
                                    RestController controller,
                                    Injector injector) {
        super(settings, client, injector);
        controller.registerHandler(RestRequest.Method.GET, "/_river/jdbc/{river}/state", this);
    }

    @Override
    public void handleRequest(RestRequest request, RestChannel channel) {
        try {
            String riverName = request.param("river");
            boolean found = false;
            for (Map.Entry<RiverName, River> entry : rivers(injector).entrySet()) {
                RiverName name = entry.getKey();
                if (name.getName().equals(riverName)) {
                    if (!name.getType().equals(JDBCRiver.TYPE)) {
                        respond(false, request, channel,
                                "River '" + riverName + "' is not a jdbc-river, but has type " + name.getType(),
                                RestStatus.UNPROCESSABLE_ENTITY
                        );
                        return;
                    }
                    JDBCRiver jdbcRiver = (JDBCRiver) entry.getValue();
                    XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
                    builder.startObject();
                    builder.field("success", true);
                    builder.field("state");
                    jdbcRiver.riverFlow().riverState().toXContent(builder, ToXContent.EMPTY_PARAMS);
                    builder.endObject();
                    channel.sendResponse(new XContentRestResponse(request, RestStatus.OK, builder));
                    found = true;
                    break;
                }
            }
            String error = found ? null : "River not found: " + riverName;
            respond(found, request, channel, error, RestStatus.OK);
        } catch (IOException ioe) {
            try {
                channel.sendResponse(new XContentThrowableRestResponse(request, ioe));
            } catch (IOException e) {
                logger.error("unable to send response to client");
            }
        }
    }

}
