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
package org.xbib.elasticsearch.plugin.jdbc.client;

import org.elasticsearch.action.admin.indices.mapping.delete.DeleteMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.util.Map;

import static org.elasticsearch.common.collect.Maps.newHashMap;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class ConfigHelper {

    private ImmutableSettings.Builder settingsBuilder;

    private Settings settings;

    private Map<String, String> mappings = newHashMap();

    public ConfigHelper reset() {
        settingsBuilder = ImmutableSettings.settingsBuilder();
        return this;
    }

    public ConfigHelper settings(Settings settings) {
        this.settings = settings;
        return this;
    }

    public ConfigHelper setting(String key, String value) {
        if (settingsBuilder == null) {
            settingsBuilder = ImmutableSettings.settingsBuilder();
        }
        settingsBuilder.put(key, value);
        return this;
    }

    public ConfigHelper setting(String key, Boolean value) {
        if (settingsBuilder == null) {
            settingsBuilder = ImmutableSettings.settingsBuilder();
        }
        settingsBuilder.put(key, value);
        return this;
    }

    public ConfigHelper setting(String key, Integer value) {
        if (settingsBuilder == null) {
            settingsBuilder = ImmutableSettings.settingsBuilder();
        }
        settingsBuilder.put(key, value);
        return this;
    }

    public ConfigHelper setting(InputStream in) throws IOException {
        if (settingsBuilder == null) {
            settingsBuilder = ImmutableSettings.settingsBuilder();
        }
        settingsBuilder = ImmutableSettings.settingsBuilder().loadFromStream(".json", in);
        return this;
    }

    public ImmutableSettings.Builder settingsBuilder() {
        return settingsBuilder != null ? settingsBuilder : ImmutableSettings.settingsBuilder();
    }

    public Settings settings() {
        if (settings != null) {
            return settings;
        }
        if (settingsBuilder == null) {
            settingsBuilder = ImmutableSettings.settingsBuilder();
        }
        return settingsBuilder.build();
    }

    public ConfigHelper mapping(String type, InputStream in) throws IOException {
        if (type == null) {
            return this;
        }
        StringWriter sw = new StringWriter();
        Streams.copy(new InputStreamReader(in), sw);
        mappings.put(type, sw.toString());
        return this;
    }

    public ConfigHelper mapping(String type, String mapping) {
        if (type == null) {
            return this;
        }
        this.mappings.put(type, mapping);
        return this;
    }

    public ConfigHelper putMapping(Client client, String index) {
        if (!mappings.isEmpty()) {
            for (Map.Entry<String, String> me : mappings.entrySet()) {
                client.admin().indices().putMapping(new PutMappingRequest(index).type(me.getKey()).source(me.getValue())).actionGet();
            }
        }
        return this;
    }

    public ConfigHelper deleteMapping(Client client, String index, String type) {
        client.admin().indices().deleteMapping(new DeleteMappingRequest(index).types(type)).actionGet();
        return this;
    }

    public String defaultMapping() throws IOException {
        XContentBuilder b = jsonBuilder()
                .startObject()
                .startObject("_default_")
                .field("date_detection", true);
        b.endObject()
                .endObject();
        return b.string();
    }

    public Map<String, String> mappings() {
        return mappings.isEmpty() ? null : mappings;
    }

}
