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
package org.xbib.elasticsearch.plugin.jdbc.classloader.jar;

import org.xbib.elasticsearch.plugin.jdbc.classloader.AbstractResourceHandle;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.cert.Certificate;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

public class JarResourceHandle extends AbstractResourceHandle {

    private final JarFile jarFile;

    private final JarEntry jarEntry;

    private final URL url;

    private final URL codeSource;

    public JarResourceHandle(JarFile jarFile, JarEntry jarEntry, URL codeSource) throws MalformedURLException {
        this.jarFile = jarFile;
        this.jarEntry = jarEntry;
        this.url = JarFileUrlStreamHandler.createURL(jarFile, jarEntry, codeSource);
        this.codeSource = codeSource;
    }

    public String getName() {
        return jarEntry.getName();
    }

    public URL getUrl() {
        return url;
    }

    public URL getCodeSourceUrl() {
        return codeSource;
    }

    public boolean isDirectory() {
        return jarEntry.isDirectory();
    }

    public InputStream getInputStream() throws IOException {
        return jarFile.getInputStream(jarEntry);
    }

    public int getContentLength() {
        return (int) jarEntry.getSize();
    }

    @Override
    public Manifest getManifest() throws IOException {
        return jarFile.getManifest();
    }

    @Override
    public Attributes getAttributes() throws IOException {
        return jarEntry.getAttributes();
    }

    @Override
    public Certificate[] getCertificates() {
        return jarEntry.getCertificates();
    }
}
