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

import org.elasticsearch.common.settings.Settings;

public interface Context<S extends Source, T extends Sink> {

    enum State { BEFORE_FETCH, FETCH, AFTER_FETCH, IDLE, EXCEPTION }

    String strategy();

    Context newInstance();

    /**
     * Set the settings
     *
     * @param settings the settings
     * @return this context
     */
    Context setSettings(Settings settings);

    /**
     * Get the settings
     *
     * @return the settings
     */
    Settings getSettings();

    /**
     * Set source
     *
     * @param source the source
     * @return this context
     */
    Context setSource(S source);

    /**
     * Get source
     *
     * @return the source
     */
    S getSource();

    /**
     * Set sink
     *
     * @param sink the sink
     * @return this context
     */
    Context setSink(T sink);

    /**
     * Get sink
     *
     * @return the sink
     */
    T getSink();

    void execute() throws Exception;

    void beforeFetch() throws Exception;

    void fetch() throws Exception;

    void afterFetch() throws Exception;

    State getState();

    void log();

    void shutdown();
}
