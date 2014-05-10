package org.xbib.elasticsearch.plugin.river.jdbc;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.river.RiversModule;

public class JDBCRiverPlugin extends AbstractPlugin {

    @Inject
    public JDBCRiverPlugin() {
    }

    @Override
    public String name() {
        return "jdbc-"
                + Build.getInstance().getVersion() + "-"
                + Build.getInstance().getShortHash();
    }

    @Override
    public String description() {
        return "JDBC plugin";
    }

    /**
     * Register the river
     *
     * @param module the rivers module
     */
    public void onModule(RiversModule module) {
        module.registerRiver("jdbc", JDBCRiverModule.class);
    }

}
