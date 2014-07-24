package org.xbib.elasticsearch.rest.action.river.execute;

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
import org.xbib.elasticsearch.action.river.jdbc.execute.RiverExecuteAction;
import org.xbib.elasticsearch.action.river.jdbc.execute.RiverExecuteRequest;
import org.xbib.elasticsearch.action.river.jdbc.execute.RiverExecuteResponse;

import static org.elasticsearch.rest.RestStatus.OK;

/**
 * Run a river. The river can be executed once with such a call. Example:
 * <p/>
 * curl -XPOST 'localhost:9200/_river/my_jdbc_river/_execute'
 */
public class RestRiverExecuteAction extends BaseRestHandler {

    @Inject
    public RestRiverExecuteAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(RestRequest.Method.POST, "/_river/jdbc/{riverName}/_execute", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) {
        String riverName = request.param("riverName");
        String riverType = "jdbc";
        RiverExecuteRequest riverExecuteRequest = new RiverExecuteRequest()
                .setRiverName(riverName)
                .setRiverType(riverType);
        client.admin().cluster().execute(RiverExecuteAction.INSTANCE, riverExecuteRequest, new RestBuilderListener<RiverExecuteResponse>(channel) {
            @Override
            public RestResponse buildResponse(RiverExecuteResponse riverExecuteResponse, XContentBuilder builder) throws Exception {
                boolean isExecuted = false;
                for (int i = 0; i < riverExecuteResponse.isExecuted().length; i++) {
                    isExecuted = isExecuted || riverExecuteResponse.isExecuted()[i];
                }
                builder.field("executed", isExecuted);
                return new BytesRestResponse(OK, builder);
            }
        });
    }

}
