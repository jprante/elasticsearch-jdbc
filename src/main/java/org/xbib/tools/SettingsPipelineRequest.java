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
package org.xbib.tools;

import org.elasticsearch.common.settings.Settings;
import org.xbib.pipeline.PipelineRequest;

public class SettingsPipelineRequest implements PipelineRequest<Settings> {

    private Settings settings;

    @Override
    public Settings get() {
        return settings;
    }

    @Override
    public SettingsPipelineRequest set(Settings settings) {
        this.settings = settings;
        return this;
    }

    @Override
    public String toString() {
        return settings.getAsMap().toString();
    }
}