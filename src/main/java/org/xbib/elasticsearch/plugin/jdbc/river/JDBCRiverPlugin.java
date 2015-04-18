/*
 * Copyright (C) 2014 Jörg Prante
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.xbib.elasticsearch.plugin.jdbc.river;

import org.elasticsearch.action.ActionModule;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.rest.RestModule;
import org.elasticsearch.river.RiversModule;
import org.xbib.elasticsearch.action.plugin.jdbc.run.RunRiverAction;
import org.xbib.elasticsearch.action.plugin.jdbc.run.TransportRunRiverAction;
import org.xbib.elasticsearch.action.plugin.jdbc.state.delete.DeleteRiverStateAction;
import org.xbib.elasticsearch.action.plugin.jdbc.state.delete.TransportDeleteRiverStateAction;
import org.xbib.elasticsearch.action.plugin.jdbc.state.get.GetRiverStateAction;
import org.xbib.elasticsearch.action.plugin.jdbc.state.get.TransportGetRiverStateAction;
import org.xbib.elasticsearch.action.plugin.jdbc.state.post.PostRiverStateAction;
import org.xbib.elasticsearch.action.plugin.jdbc.state.post.TransportPostRiverStateAction;
import org.xbib.elasticsearch.action.plugin.jdbc.state.put.PutRiverStateAction;
import org.xbib.elasticsearch.action.plugin.jdbc.state.put.TransportPutRiverStateAction;
import org.xbib.elasticsearch.plugin.jdbc.state.RiverStateModule;
import org.xbib.elasticsearch.plugin.jdbc.state.RiverStateService;
import org.xbib.elasticsearch.rest.action.river.jdbc.RestRiverStateAction;
import org.xbib.elasticsearch.rest.action.river.jdbc.RestRunRiverAction;

import java.util.Collection;

import static org.elasticsearch.common.collect.Lists.newArrayList;

public class JDBCRiverPlugin extends AbstractPlugin {

    private final Settings settings;

    @Inject
    public JDBCRiverPlugin(Settings settings) {
        this.settings = settings;
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
        // if we are in feeder node mode, we skip initiating the server-side only river state module
        if (!"feeder".equals(settings.get("name"))) {
            modules.add(RiverStateModule.class);
        }
        return modules;
    }

    @Override
    public Collection<Class<? extends LifecycleComponent>> services() {
        Collection<Class<? extends LifecycleComponent>> services = newArrayList();
        // if we are in feeder node mode, we skip starting the server-side only river state module
        if (!"feeder".equals(settings.get("name"))) {
            services.add(RiverStateService.class);
        }
        return services;
    }

    public void onModule(RiversModule module) {
        module.registerRiver("jdbc", JDBCRiverModule.class);
    }

    public void onModule(ActionModule module) {
        module.registerAction(DeleteRiverStateAction.INSTANCE, TransportDeleteRiverStateAction.class);
        module.registerAction(PutRiverStateAction.INSTANCE, TransportPutRiverStateAction.class);
        module.registerAction(PostRiverStateAction.INSTANCE, TransportPostRiverStateAction.class);
        module.registerAction(GetRiverStateAction.INSTANCE, TransportGetRiverStateAction.class);
        module.registerAction(RunRiverAction.INSTANCE, TransportRunRiverAction.class);
    }

    public void onModule(RestModule module) {
        module.addRestAction(RestRunRiverAction.class);
        module.addRestAction(RestRiverStateAction.class);
    }

}
