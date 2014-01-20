
package org.xbib.elasticsearch.rest.action;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;

import org.xbib.elasticsearch.river.jdbc.JDBCRiver;

import java.util.Map;

/**
 * The JDBC River REST inducer. The river can be fired once to run
 * when this move is called from REST.
 * <p/>
 * Example:<br/>
 * <code>
 * curl -XPOST 'localhost:9200/_river/my_jdbc_river/_induce'
 * </code>
 */
public class RestJDBCRiverInduceAction extends AbstractRestRiverAction {

    @Inject
    public RestJDBCRiverInduceAction(Settings settings, Client client,
                                     RestController controller, Injector injector) {
        super(settings, client, injector);
        controller.registerHandler(RestRequest.Method.POST, "/_river/jdbc/{river}/induce", this);
    }

    @Override
    public void handleRequest(final RestRequest request, RestChannel channel) {
        //Get and check river name parameter
        String riverName = request.param("river");
        if (riverName == null || riverName.isEmpty()) {
            respond(false, request, channel, "Parameter 'river' is required", RestStatus.BAD_REQUEST);
            return;
        }
        // find river class
        //GetResponse get = client.get(Requests.getRequest("_river").type(riverName).id("_meta")).actionGet();
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
                jdbcRiver.induce();
                found = true;
                break;
            }
        }
        String error = found ? null : "River not found: " + riverName;
        respond(found, request, channel, error, RestStatus.OK);
    }

}
