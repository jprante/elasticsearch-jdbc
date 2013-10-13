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
package org.xbib.elasticsearch.river.jdbc;

import org.xbib.elasticsearch.river.jdbc.support.RiverContext;

import java.util.Date;

/**
 * RiverFlow fluent API
 *
 * The RiverFlow is the abstraction to the thread which
 * performs data fetching from the river source and transports it
 * to the river mouth
 *
 * @author Jörg Prante <joergprante@gmail.com>
 */
public interface RiverFlow extends Runnable {

    /**
     * The doc ID of the info document in the river index
     */
    String ID_INFO_RIVER_INDEX = "_custom";

    /**
     * The strategy
     *
     * @return the strategy of this river task
     */
    String strategy();

    /**
     * Set river context
     *
     * @param context the context
     * @return this river flow
     */
    RiverFlow riverContext(RiverContext context);

    /**
     * Get river context
     *
     * @return the river context
     */
    RiverContext riverContext();

    /**
     * Set river connection start date
     *
     * @param creationDate the creation date
     * @return this river flow
     */
    RiverFlow startDate(Date creationDate);

    /**
     * Get river connection start date
     *
     * @return the date
     */
    Date startDate();

    /**
     * Run river once
     */
    void move();

    /**
     * Delay between river actions.
     *
     * @param reason the reson
     * @return this river flow
     */
    RiverFlow delay(String reason);

    /**
     * Abort river task. Set signal to interrupt thread and free resources.
     */
    void abort();

}

