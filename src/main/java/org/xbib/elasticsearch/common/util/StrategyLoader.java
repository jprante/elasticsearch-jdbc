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

import org.xbib.elasticsearch.jdbc.strategy.Flow;
import org.xbib.elasticsearch.jdbc.strategy.JDBCSource;
import org.xbib.elasticsearch.jdbc.strategy.Source;
import org.xbib.elasticsearch.jdbc.strategy.Mouth;
import org.xbib.elasticsearch.jdbc.strategy.standard.StandardFlow;
import org.xbib.elasticsearch.jdbc.strategy.standard.StandardSource;
import org.xbib.elasticsearch.jdbc.strategy.standard.StandardMouth;

import java.util.ServiceLoader;

/**
 * The strategy loader
 */
public class StrategyLoader {

    /**
     * A flow encapsulates the thread that moves the data from source to mouth
     *
     * @param strategy the strategy
     * @return a flow, or the StandardFlow
     */
    public static Flow newFlow(String strategy) {
        ServiceLoader<Flow> loader = ServiceLoader.load(Flow.class);
        for (Flow flow : loader) {
            if (strategy.equals(flow.strategy())) {
                return flow.newInstance();
            }
        }
        return new StandardFlow();
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
     * A mouth is the Elasticsearch side where the bulk processor lives
     *
     * @param strategy the strategy
     * @return a new instance of a mouth, or an instance of the StandardMouth if strategy does not exist
     */
    public static Mouth newMouth(String strategy) {
        ServiceLoader<Mouth> loader = ServiceLoader.load(Mouth.class);
        for (Mouth mouth : loader) {
            if (strategy.equals(mouth.strategy())) {
                return mouth.newInstance();
            }
        }
        return new StandardMouth();
    }

}
