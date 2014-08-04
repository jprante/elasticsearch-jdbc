package org.xbib.elasticsearch.action.river.jdbc.state;

import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.inject.AbstractModule;

public class RiverStateModule extends AbstractModule {

    /**
     * Register metadata factory in Elasticsearch
     */
    static {
        MetaData.registerFactory(RiverStatesMetaData.TYPE, RiverStatesMetaData.FACTORY);
    }

    /**
     * Only one RiverStateService instance is allowed
     */
    @Override
    protected void configure() {
        bind(RiverStateService.class).asEagerSingleton();
    }
}
