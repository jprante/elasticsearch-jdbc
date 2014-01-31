
package org.xbib.elasticsearch.river.jdbc;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.river.River;

/**
 * The JDBC river module
 */
public class JDBCRiverModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(River.class).to(JDBCRiver.class).asEagerSingleton();
    }
}
