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
package org.xbib.elasticsearch.jdbc.strategy;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.metrics.MeterMetric;
import org.elasticsearch.common.settings.Settings;
import org.xbib.elasticsearch.common.client.IngestFactory;

import java.util.Queue;

/**
 * A Flow is the controlling component for creating contexts
 * which can be processed independently from each other.
 *
 * @param <C>
 */
public interface Flow<C extends Context> {

    /**
     * The name of the strategy this flow belongs to
     *
     * @return the strategy name
     */
    String strategy();

    Flow<C> newInstance();

    /**
     * Create a new context for a run
     *
     * @return a new context
     */
    C newContext();

    /**
     * Sets the name
     *
     * @param name the name
     * @return this name
     */
    Flow setName(String name);

    /**
     * Gets the name
     *
     * @return the name
     */
    String getName();

    /**
     * Set the settings
     *
     * @param settings the settings
     * @return this flow
     */
    Flow setSettings(Settings settings);

    /**
     * Get the settings
     *
     * @return the settings
     */
    Settings getSettings();

    /**
     * Set ingest factory
     *
     * @param ingestFactory ingest factory
     * @return this flow
     */
    Flow setIngestFactory(IngestFactory ingestFactory);

    Flow setClient(Client client);

    /**
     * Get the client
     *
     * @return the client
     */
    Client getClient();

    /**
     * Log metrics
     *
     * @param cause the cause why metrics are logged
     */
    void logMetrics(Context context, String cause);

    /**
     * Execute this flow
     *
     * @throws Exception
     */
    void execute(C context) throws Exception;

    /**
     * Set metric
     *
     * @param meterMetric the meter metric
     * @return this flow
     */
    Flow setMetric(MeterMetric meterMetric);

    /**
     * Get metric
     *
     * @return metric
     */
    MeterMetric getMetric();

    /**
     * Set queue for processing Context requests
     *
     * @param queue the queue
     * @return this flow
     */
    Flow setQueue(Queue<Context> queue);

    /**
     * Get queue for context processing
     *
     * @return the queue for processing context requests
     */
    Queue<Context> getQueue();

    boolean isMetricThreadEnabled();

    boolean isSuspensionThreadEnabled();

}
