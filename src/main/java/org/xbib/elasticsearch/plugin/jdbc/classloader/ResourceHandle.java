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

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.security.cert.Certificate;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

/**
 * This is a handle (a connection) to some resource, which may
 * be a class, native library, text file, image, etc. Handles are returned
 * by a ResourceFinder. A resource handle allows easy access to the resource data
 * (using methods {@link #getInputStream} or {@link #getBytes}) as well as
 * access resource metadata, such as attributes, certificates, etc.
 * <p/>
 * As soon as the handle is no longer in use, it should be explicitly
 * {@link #close}d, similarly to I/O streams.
 */
public interface ResourceHandle {
    /**
     * Return the name of the resource. The name is a "/"-separated path
     * name that identifies the resource.
     */
    String getName();

    /**
     * Returns the URL of the resource.
     */
    URL getUrl();

    /**
     * Does this resource refer to a directory.  Directory resources are commly used
     * as the basis for a URL in client application.  A directory resource has 0 bytes for it's content.
     */
    boolean isDirectory();

    /**
     * Returns the CodeSource URL for the class or resource.
     */
    URL getCodeSourceUrl();

    /**
     * Returns and InputStream for reading this resource data.
     */
    InputStream getInputStream() throws IOException;

    /**
     * Returns the length of this resource data, or -1 if unknown.
     */
    int getContentLength();

    /**
     * Returns this resource data as an array of bytes.
     */
    byte[] getBytes() throws IOException;

    /**
     * Returns the Manifest of the JAR file from which this resource
     * was loaded, or null if none.
     */
    Manifest getManifest() throws IOException;

    /**
     * Return the Certificates of the resource, or null if none.
     */
    Certificate[] getCertificates();

    /**
     * Return the Attributes of the resource, or null if none.
     */
    Attributes getAttributes() throws IOException;

    /**
     * Closes a connection to the resource indentified by this handle. Releases
     * any I/O objects associated with the handle.
     */
    void close();
}
