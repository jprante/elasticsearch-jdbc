package org.xbib.elasticsearch.rest.action;

import java.io.IOException;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.XContentRestResponse;
import org.elasticsearch.rest.XContentThrowableRestResponse;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;

import org.xbib.elasticsearch.river.jdbc.JDBCRiver;

import static org.elasticsearch.rest.RestStatus.OK;
import static org.elasticsearch.rest.action.support.RestXContentBuilder.restContentBuilder;

public class RestJDBCRiverCreateAction extends AbstractRestRiverAction {

    private Settings settings = ImmutableSettings.EMPTY;

    @Inject
    public RestJDBCRiverCreateAction(Settings settings, Client client,
                                     RestController controller, Injector injector) {
        super(settings, client, injector);
        this.settings = settings;
        controller.registerHandler(RestRequest.Method.POST, "/_river/jdbc/{river}/create", this);
    }

    @Override
    public void handleRequest(RestRequest request, RestChannel channel) {
        try {
            String riverName = request.param("river");
            RiverSettings riverSettings = new RiverSettings(settings,
                    XContentHelper.convertToMap(request.content(), true).v2())  ;
            JDBCRiver river = new JDBCRiver(new RiverName("jdbc", riverName), riverSettings, client);
            river.start();
            XContentBuilder builder = restContentBuilder(request);
            builder.startObject()
                    .field("active", river.riverFlow().isActive())
                    .endObject();
            channel.sendResponse(new XContentRestResponse(request, OK, builder));
        } catch (IOException ioe) {
            try {
                channel.sendResponse(new XContentThrowableRestResponse(request, ioe));
            } catch (IOException e) {
                logger.error("unable to send response to client");
            }
        }
    }

}
