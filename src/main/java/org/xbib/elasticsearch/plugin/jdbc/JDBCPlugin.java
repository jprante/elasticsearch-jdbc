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
package org.xbib.elasticsearch.plugin.jdbc;

import org.elasticsearch.action.ActionModule;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.rest.RestModule;
import org.xbib.elasticsearch.action.jdbc.execute.ExecuteTaskAction;
import org.xbib.elasticsearch.action.jdbc.execute.TransportExecuteTaskAction;
import org.xbib.elasticsearch.action.jdbc.state.delete.DeleteStateAction;
import org.xbib.elasticsearch.action.jdbc.state.delete.TransportDeleteStateAction;
import org.xbib.elasticsearch.action.jdbc.state.get.GetStateAction;
import org.xbib.elasticsearch.action.jdbc.state.get.TransportGetStateAction;
import org.xbib.elasticsearch.action.jdbc.state.post.PostStateAction;
import org.xbib.elasticsearch.action.jdbc.state.post.TransportPostStateAction;
import org.xbib.elasticsearch.action.jdbc.state.put.PutStateAction;
import org.xbib.elasticsearch.action.jdbc.state.put.TransportPutStateAction;
import org.xbib.elasticsearch.jdbc.state.cluster.StateModule;
import org.xbib.elasticsearch.jdbc.state.cluster.StateService;
import org.xbib.elasticsearch.rest.action.jdbc.RestRunAction;
import org.xbib.elasticsearch.rest.action.jdbc.RestStateAction;

import java.util.Collection;

import static org.elasticsearch.common.collect.Lists.newArrayList;

public class JDBCPlugin extends AbstractPlugin {

    private final Settings settings;

    @Inject
    public JDBCPlugin(Settings settings) {
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
        // if we are in feeder node mode, we skip initiating the server-side only state module
        if (!"feeder".equals(settings.get("name"))) {
            modules.add(StateModule.class);
        }
        return modules;
    }

    @Override
    public Collection<Class<? extends LifecycleComponent>> services() {
        Collection<Class<? extends LifecycleComponent>> services = newArrayList();
        // if we are in feeder node mode, we skip starting the server-side only state module
        if (!"feeder".equals(settings.get("name"))) {
            services.add(StateService.class);
        }
        return services;
    }

    public void onModule(ActionModule module) {
        module.registerAction(DeleteStateAction.INSTANCE, TransportDeleteStateAction.class);
        module.registerAction(PutStateAction.INSTANCE, TransportPutStateAction.class);
        module.registerAction(PostStateAction.INSTANCE, TransportPostStateAction.class);
        module.registerAction(GetStateAction.INSTANCE, TransportGetStateAction.class);
        module.registerAction(ExecuteTaskAction.INSTANCE, TransportExecuteTaskAction.class);
    }

    public void onModule(RestModule module) {
        module.addRestAction(RestRunAction.class);
        module.addRestAction(RestStateAction.class);
    }

}
