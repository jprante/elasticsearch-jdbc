package org.xbib.elasticsearch.plugin.river.jdbc;

import org.elasticsearch.action.ActionModule;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.rest.RestModule;
import org.elasticsearch.river.RiversModule;
import org.xbib.elasticsearch.action.river.jdbc.execute.RiverExecuteAction;
import org.xbib.elasticsearch.action.river.jdbc.execute.TransportRiverExecuteAction;
import org.xbib.elasticsearch.action.river.jdbc.state.RiverStateModule;
import org.xbib.elasticsearch.action.river.jdbc.state.delete.DeleteRiverStateAction;
import org.xbib.elasticsearch.action.river.jdbc.state.delete.TransportDeleteRiverStateAction;
import org.xbib.elasticsearch.action.river.jdbc.state.get.GetRiverStateAction;
import org.xbib.elasticsearch.action.river.jdbc.state.get.TransportGetRiverStateAction;
import org.xbib.elasticsearch.action.river.jdbc.state.put.PutRiverStateAction;
import org.xbib.elasticsearch.action.river.jdbc.state.put.TransportPutRiverStateAction;
import org.xbib.elasticsearch.rest.action.river.execute.RestRiverExecuteAction;
import org.xbib.elasticsearch.rest.action.river.state.get.RestRiverGetStateAction;

import java.util.Collection;

import static org.elasticsearch.common.collect.Lists.newArrayList;

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

    @Override
    public Collection<Class<? extends Module>> modules() {
        Collection<Class<? extends Module>> modules = newArrayList();
        modules.add(RiverStateModule.class);
        return modules;
    }

    public void onModule(RiversModule module) {
        module.registerRiver("jdbc", JDBCRiverModule.class);
    }

    public void onModule(ActionModule module) {
        module.registerAction(RiverExecuteAction.INSTANCE, TransportRiverExecuteAction.class);
        module.registerAction(GetRiverStateAction.INSTANCE, TransportGetRiverStateAction.class);
        module.registerAction(PutRiverStateAction.INSTANCE, TransportPutRiverStateAction.class);
        module.registerAction(DeleteRiverStateAction.INSTANCE, TransportDeleteRiverStateAction.class);
    }

    public void onModule(RestModule module) {
        module.addRestAction(RestRiverExecuteAction.class);
        module.addRestAction(RestRiverGetStateAction.class);
    }

}
