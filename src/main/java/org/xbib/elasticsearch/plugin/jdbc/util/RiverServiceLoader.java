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
package org.xbib.elasticsearch.plugin.jdbc.util;

import org.xbib.elasticsearch.river.jdbc.RiverFlow;
import org.xbib.elasticsearch.river.jdbc.RiverMouth;
import org.xbib.elasticsearch.river.jdbc.RiverSource;
import org.xbib.elasticsearch.river.jdbc.strategy.simple.SimpleRiverFlow;
import org.xbib.elasticsearch.river.jdbc.strategy.simple.SimpleRiverMouth;
import org.xbib.elasticsearch.river.jdbc.strategy.simple.SimpleRiverSource;

import java.util.ServiceLoader;

/**
 * The river service loader
 */
public class RiverServiceLoader {

    /**
     * A river flow encapsulates the thread that moves the data from source to mouth
     *
     * @param strategy the strategy
     * @return a river flow, or the SimpleRiverFlow
     */
    public static RiverFlow newRiverFlow(String strategy) {
        ServiceLoader<RiverFlow> riverFlowServiceLoader = ServiceLoader.load(RiverFlow.class);
        for (RiverFlow riverFlow : riverFlowServiceLoader) {
            if (strategy.equals(riverFlow.strategy())) {
                return riverFlow.newInstance();
            }
        }
        return new SimpleRiverFlow();
    }

    /**
     * A river source is the origin, the data producing side
     *
     * @param strategy the strategy
     * @return a river source, or the SimpleRiverSource
     */
    public static RiverSource newRiverSource(String strategy) {
        ServiceLoader<RiverSource> sourceLoader = ServiceLoader.load(RiverSource.class);
        for (RiverSource riverSource : sourceLoader) {
            if (strategy.equals(riverSource.strategy())) {
                return riverSource.newInstance();
            }
        }
        return new SimpleRiverSource();
    }

    /**
     * A river mouth is the Elasticsearch side of the river, where the bulk processor lives
     *
     * @param strategy the strategy
     * @return a new instance of a river mouth, or an instance of the SimpleRiverMouth if strategy does not exist
     */
    public static RiverMouth newRiverMouth(String strategy) {
        ServiceLoader<RiverMouth> riverMouthLoader = ServiceLoader.load(RiverMouth.class);
        for (RiverMouth riverMouth : riverMouthLoader) {
            if (strategy.equals(riverMouth.strategy())) {
                return riverMouth.newInstance();
            }
        }
        return new SimpleRiverMouth();
    }

}
