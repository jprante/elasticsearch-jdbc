package org.xbib.elasticsearch.rest.action.river.state.get;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.support.RestBuilderListener;
import org.xbib.elasticsearch.action.river.jdbc.state.RiverState;
import org.xbib.elasticsearch.action.river.jdbc.state.get.GetRiverStateAction;
import org.xbib.elasticsearch.action.river.jdbc.state.get.GetRiverStateRequest;
import org.xbib.elasticsearch.action.river.jdbc.state.get.GetRiverStateResponse;

import static org.elasticsearch.rest.RestStatus.OK;

public class RestRiverGetStateAction extends BaseRestHandler {

    @Inject
    public RestRiverGetStateAction(Settings settings, Client client, RestController controller) {
        super(settings, client);

        controller.registerHandler(RestRequest.Method.GET, "/_river/jdbc/{riverName}/_state", this);
    }

    @Override
    public void handleRequest(RestRequest request, RestChannel channel) {
        String riverName = request.param("riverName");
        String riverType = "jdbc";
        GetRiverStateRequest riverStateRequest = new GetRiverStateRequest();
        riverStateRequest.setRiverName(riverName).setRiverType(riverType);
        client.admin().cluster().execute(GetRiverStateAction.INSTANCE, riverStateRequest, new RestBuilderListener<GetRiverStateResponse>(channel) {

            @Override
            public RestResponse buildResponse(GetRiverStateResponse getRiverStateResponse, XContentBuilder builder) throws Exception {
                builder.startObject();
                builder.startArray("state");
                for (RiverState state : getRiverStateResponse.getStates()) {
                    builder.startObject()
                        .field("name", state.getName())
                        .field("type", state.getType())
                        .field("settings", state.getSettings().getAsMap())
                        .field("map").map(state.getMap())
                        .endObject();
                }
                builder.endArray().endObject();
                return new BytesRestResponse(OK, builder);
            }
        });
    }

}
