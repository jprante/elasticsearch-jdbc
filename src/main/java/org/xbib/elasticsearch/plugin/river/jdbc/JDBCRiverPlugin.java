
package org.xbib.elasticsearch.plugin.river.jdbc;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.rest.RestModule;
import org.elasticsearch.river.RiversModule;
import org.xbib.elasticsearch.rest.action.RestJDBCRiverInduceAction;
import org.xbib.elasticsearch.river.jdbc.JDBCRiver;
import org.xbib.elasticsearch.river.jdbc.JDBCRiverModule;

/**
 * JDBC River plugin. This plugin is the enrty point for the
 * Elasticsearch river service. The onModule() methods are used
 * by reflection. The JDBC River Module - a simple one which just
 * binds a JDBCRiver instance to a River interface - and the
 * REST move is configured. Also, a concurrent bulk move
 * is registered.
 *
 * @author Jörg Prante <joergprante@gmail.com>
 */
public class JDBCRiverPlugin extends AbstractPlugin {

    @Inject
    public JDBCRiverPlugin() {
    }

    @Override
    public String name() {
        return JDBCRiver.NAME;
    }

    @Override
    public String description() {
        return "JDBC River";
    }

    /**
     * Register the JDBC river to Elasticsearch node
     *
     * @param module
     */
    public void onModule(RiversModule module) {
        module.registerRiver(JDBCRiver.TYPE, JDBCRiverModule.class);
    }

    /**
     * Register the REST move to Elasticsearch node
     *
     * @param module
     */
    public void onModule(RestModule module) {
        module.addRestAction(RestJDBCRiverInduceAction.class);
    }

}
