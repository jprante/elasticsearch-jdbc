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
package org.xbib.elasticsearch.river.jdbc;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.metrics.MeterMetric;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.river.RiverName;
import org.xbib.elasticsearch.plugin.jdbc.client.IngestFactory;

import java.util.Queue;

/**
 * A RiverFlow is the controlling component for creatin RiverContexts
 * which can be processed independently from each other. By doing this,
 * RiverContexts can be processed
 *
 * @param <RC>
 */
public interface RiverFlow<RC extends RiverContext> {

    /**
     * The name of the strategy the river flow belongs to
     *
     * @return the strategy name
     */
    String strategy();

    RiverFlow<RC> newInstance();

    /**
     * Create a new river context for a river run
     *
     * @return a new river context
     */
    RC newRiverContext();

    /**
     * Sets the river name
     *
     * @param riverName the river name
     * @return this river name
     */
    RiverFlow setRiverName(RiverName riverName);

    /**
     * Gets the river name
     *
     * @return the river name
     */
    RiverName getRiverName();

    /**
     * Set the settings
     *
     * @param settings the settings
     * @return this river flow
     */
    RiverFlow setSettings(Settings settings);

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
     * @return this river flow
     */
    RiverFlow setIngestFactory(IngestFactory ingestFactory);

    RiverFlow setClient(Client client);

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
    void logMetrics(RC riverContext, String cause);

    /**
     * Execute this river flow
     *
     * @throws Exception
     */
    void execute(RC riverContext) throws Exception;

    /**
     * Set metric
     *
     * @param meterMetric the meter metric
     * @return this river flow
     */
    RiverFlow setMetric(MeterMetric meterMetric);

    /**
     * Get metric
     *
     * @return river metric
     */
    MeterMetric getMetric();

    /**
     * Set queue for processing RiverContext requests
     *
     * @param queue the queue
     * @return this river flow
     */
    RiverFlow setQueue(Queue<RiverContext> queue);

    /**
     * Get queue for RiverContext processing
     *
     * @return the queue for processing RiverContext requests
     */
    Queue<RiverContext> getQueue();

    boolean isMetricThreadEnabled();

    boolean isSuspensionThreadEnabled();

    void shutdown() throws Exception;

}
