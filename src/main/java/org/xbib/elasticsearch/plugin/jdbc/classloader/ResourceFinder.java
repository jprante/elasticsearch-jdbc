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
package org.xbib.elasticsearch.plugin.jdbc.classloader;

import java.net.URL;
import java.util.Enumeration;

/**
 * Abstraction of resource searching policy. Given resource name, the resource
 * finder performs implementation-specific lookup, and, if it is able to locate
 * the resource, returns the {@link AbstractResourceHandle handle(s)} or URL(s)
 * of it.
 */
public interface ResourceFinder {

    /**
     * Find the resource by name and return URL of it if found.
     *
     * @param name the resource name
     * @return resource URL or null if resource was not found
     */
    URL findResource(String name);

    /**
     * Find all resources with given name and return enumeration of their URLs.
     *
     * @param name the resource name
     * @return enumeration of resource URLs (possibly empty).
     */
    Enumeration<URL> findResources(String name);

    /**
     * Get the resource by name and, if found, open connection to it and return
     * the {@link AbstractResourceHandle handle} of it.
     *
     * @param name the resource name
     * @return resource handle or null if resource was not found
     */
    ResourceHandle getResource(String name);
}
