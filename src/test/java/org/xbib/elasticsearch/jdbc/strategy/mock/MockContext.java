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
package org.xbib.elasticsearch.jdbc.strategy.mock;

import org.elasticsearch.common.settings.Settings;
import org.xbib.elasticsearch.jdbc.strategy.Source;
import org.xbib.elasticsearch.jdbc.strategy.Context;
import org.xbib.elasticsearch.jdbc.strategy.Sink;

public class MockContext implements Context<Source,Sink> {

    @Override
    public String strategy() {
        return null;
    }

    @Override
    public Context newInstance() {
        return this;
    }

    @Override
    public Context setSettings(Settings settings) {
        return this;
    }

    @Override
    public Settings getSettings() {
        return null;
    }

    @Override
    public Context setSource(Source source) {
        return this;
    }

    @Override
    public Source getSource() {
        return null;
    }

    @Override
    public Context setSink(Sink sink) {
        return this;
    }

    @Override
    public Sink getSink() {
        return null;
    }

    @Override
    public void execute() throws Exception {
    }

    @Override
    public void beforeFetch() throws Exception {
    }

    @Override
    public void fetch() throws Exception {
    }

    @Override
    public void afterFetch() throws Exception {
    }

    @Override
    public State getState() {
        // always idle
        return State.IDLE;
    }

    @Override
    public void log() {
    }

    @Override
    public void shutdown() {
    }
}
