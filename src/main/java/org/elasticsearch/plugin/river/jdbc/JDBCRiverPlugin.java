/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.plugin.river.jdbc;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.rest.RestModule;
import org.elasticsearch.rest.action.RestJDBCRiverInduceAction;
import org.elasticsearch.river.RiversModule;
import org.elasticsearch.river.jdbc.JDBCRiver;
import org.elasticsearch.river.jdbc.JDBCRiverModule;

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
        module.registerRiver("jdbc", JDBCRiverModule.class);
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
