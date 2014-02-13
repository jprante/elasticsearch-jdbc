
package org.xbib.elasticsearch.plugin.river.jdbc;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.rest.RestModule;
import org.elasticsearch.river.RiversModule;

import org.xbib.elasticsearch.rest.action.RestJDBCRiverCreateAction;
import org.xbib.elasticsearch.rest.action.RestJDBCRiverInduceAction;
import org.xbib.elasticsearch.rest.action.RestJDBCRiverStateAction;
import org.xbib.elasticsearch.river.jdbc.JDBCRiver;
import org.xbib.elasticsearch.river.jdbc.JDBCRiverModule;

/**
 * JDBC River plugin. This plugin is the enrty point for the
 * Elasticsearch river service. The onModule() methods are used
 * by reflection. The JDBC River Module - a simple one which just
 * binds a JDBCRiver instance to a River interface - and the
 * REST move is configured. Also, a concurrent bulk move
 * is registered.
 */
public class JDBCRiverPlugin extends AbstractPlugin {

    @Inject
    public JDBCRiverPlugin() {
    }

    @Override
    public String name() {
        return JDBCRiver.NAME + "-"
                + Build.getInstance().getVersion() + "-"
                + Build.getInstance().getShortHash();
    }

    @Override
    public String description() {
        return "JDBC River";
    }

    /**
     * Register the river to Elasticsearch
     *
     * @param module the rivers module
     */
    public void onModule(RiversModule module) {
        module.registerRiver(JDBCRiver.TYPE, JDBCRiverModule.class);
    }

    /**
     * Register the REST actions of this river
     *
     * @param module the REST module
     */
    public void onModule(RestModule module) {
        module.addRestAction(RestJDBCRiverCreateAction.class);
        module.addRestAction(RestJDBCRiverInduceAction.class);
        module.addRestAction(RestJDBCRiverStateAction.class);
    }

}
