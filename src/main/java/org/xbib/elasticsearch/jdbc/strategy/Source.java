/*
 * Copyright (C) 2015 Jörg Prante
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
package org.xbib.elasticsearch.jdbc.strategy;

import org.xbib.elasticsearch.common.metrics.SourceMetric;

import java.io.IOException;

public interface Source<C extends Context> {

    /**
     * The strategy this source supports.
     *
     * @return the strategy as a string
     */
    String strategy();

    /**
     * Create new source instance
     *
     * @return a new source instance
     */
    Source<C> newInstance();

    /**
     * Set the context
     *
     * @param context the context
     * @return this source
     */
    Source<C> setContext(C context);

    C getContext();

    /**
     * Executed before fetch() is executed
     *
     * @throws Exception when execution fails
     */
    void beforeFetch() throws Exception;

    /**
     * Fetch a data portion from the database and pass it to the task
     * for further processing.
     *
     * @throws Exception when execution gives an error
     */
    void fetch() throws Exception;

    /**
     * Executed after fetch() has been executed or threw an exception.
     *
     * @throws Exception when execution fails
     */
    void afterFetch() throws Exception;

    /**
     * Shutdown source
     *
     * @throws IOException when shutdown fails
     */
    void shutdown() throws IOException;

    SourceMetric getMetric();
}
