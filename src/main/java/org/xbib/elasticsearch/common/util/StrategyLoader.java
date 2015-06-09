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
package org.xbib.elasticsearch.common.util;

import org.xbib.elasticsearch.jdbc.strategy.Context;
import org.xbib.elasticsearch.jdbc.strategy.JDBCSource;
import org.xbib.elasticsearch.jdbc.strategy.Source;
import org.xbib.elasticsearch.jdbc.strategy.Sink;
import org.xbib.elasticsearch.jdbc.strategy.standard.StandardContext;
import org.xbib.elasticsearch.jdbc.strategy.standard.StandardSource;
import org.xbib.elasticsearch.jdbc.strategy.standard.StandardSink;

import java.util.ServiceLoader;

/**
 * The strategy loader
 */
public class StrategyLoader {

    /**
     * A context encapsulates the move from source to sink
     *
     * @param strategy the strategy
     * @return a context, or the StandardContext
     */
    public static Context newContext(String strategy) {
        ServiceLoader<Context> loader = ServiceLoader.load(Context.class);
        for (Context context : loader) {
            if (strategy.equals(context.strategy())) {
                return context.newInstance();
            }
        }
        return new StandardContext();
    }

    /**
     * Load a new source
     *
     * @param strategy the strategy
     * @return a source, or the StandardSource
     */
    public static Source newSource(String strategy) {
        ServiceLoader<Source> loader = ServiceLoader.load(Source.class);
        for (Source source : loader) {
            if (strategy.equals(source.strategy())) {
                return source.newInstance();
            }
        }
        return new StandardSource();
    }


    /**
     * Load a new JDBC source
     *
     * @param strategy the strategy
     * @return a source, or the StandardSource
     */
    public static JDBCSource newJDBCSource(String strategy) {
        ServiceLoader<JDBCSource> loader = ServiceLoader.load(JDBCSource.class);
        for (JDBCSource source : loader) {
            if (strategy.equals(source.strategy())) {
                return source.newInstance();
            }
        }
        return new StandardSource();
    }

    /**
     * A sink is the Elasticsearch side where the bulk processor lives
     *
     * @param strategy the strategy
     * @return a new instance of a sink, or an instance of the StandardSinkif strategy does not exist
     */
    public static Sink newSink(String strategy) {
        ServiceLoader<Sink> loader = ServiceLoader.load(Sink.class);
        for (Sink sink : loader) {
            if (strategy.equals(sink.strategy())) {
                return sink.newInstance();
            }
        }
        return new StandardSink();
    }

}
