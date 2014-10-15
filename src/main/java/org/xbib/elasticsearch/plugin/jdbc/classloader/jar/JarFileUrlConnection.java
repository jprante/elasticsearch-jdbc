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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.JarURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.security.Permission;
import java.security.cert.Certificate;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

public class JarFileUrlConnection extends JarURLConnection {

    public static final URL DUMMY_JAR_URL;

    static {
        try {
            DUMMY_JAR_URL = new URL("jar", "", -1, "file:dummy!/", new URLStreamHandler() {
                protected URLConnection openConnection(URL u) {
                    throw new UnsupportedOperationException();
                }
            });
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final URL url;

    private final JarFile jarFile;

    private final JarEntry jarEntry;

    private final URL jarFileUrl;

    public JarFileUrlConnection(URL url, JarFile jarFile, JarEntry jarEntry) throws MalformedURLException {
        super(DUMMY_JAR_URL);
        if (url == null) {
            throw new NullPointerException("url is null");
        }
        if (jarFile == null) {
            throw new NullPointerException("jarFile is null");
        }
        if (jarEntry == null) {
            throw new NullPointerException("jarEntry is null");
        }
        this.url = url;
        this.jarFile = jarFile;
        this.jarEntry = jarEntry;
        jarFileUrl = new File(jarFile.getName()).toURI().toURL();
    }

    @Override
    public JarFile getJarFile() throws IOException {
        if (getUseCaches()) {
            return jarFile;
        } else {
            return new JarFile(jarFile.getName());
        }
    }

    @Override
    public synchronized void connect() {
    }

    @Override
    public URL getJarFileURL() {
        return jarFileUrl;
    }

    @Override
    public String getEntryName() {
        return getJarEntry().getName();
    }

    @Override
    public Manifest getManifest() throws IOException {
        return jarFile.getManifest();
    }

    @Override
    public JarEntry getJarEntry() {
        if (getUseCaches()) {
            return jarEntry;
        } else {
            //return (JarEntry) jarEntry.clone();
            // There is a clone method, but the below way might be safer.
            return jarFile.getJarEntry(jarEntry.getName());
        }
    }

    @Override
    public Attributes getAttributes() throws IOException {
        return getJarEntry().getAttributes();
    }

    @Override
    public Attributes getMainAttributes() throws IOException {
        return getManifest().getMainAttributes();
    }

    @Override
    public Certificate[] getCertificates() throws IOException {
        return getJarEntry().getCertificates();
    }

    @Override
    public URL getURL() {
        return url;
    }

    @Override
    public int getContentLength() {
        long size = getJarEntry().getSize();
        if (size > Integer.MAX_VALUE) {
            return -1;
        }
        return (int) size;
    }

    @Override
    public long getLastModified() {
        return getJarEntry().getTime();
    }

    @Override
    public synchronized InputStream getInputStream() throws IOException {
        return jarFile.getInputStream(jarEntry);
    }

    @Override
    public Permission getPermission() throws IOException {
        URL jarFileUrl = new File(jarFile.getName()).toURI().toURL();
        return jarFileUrl.openConnection().getPermission();
    }

    @Override
    public String toString() {
        return JarFileUrlConnection.class.getName() + ":" + url;
    }
}
