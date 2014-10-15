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
package org.xbib.elasticsearch.plugin.jdbc.classloader.directory;

import org.xbib.elasticsearch.plugin.jdbc.classloader.AbstractResourceHandle;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.cert.Certificate;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

public class DirectoryResourceHandle extends AbstractResourceHandle {

    private final String name;

    private final File file;

    private final Manifest manifest;

    private final URL url;

    private final URL codeSource;

    public DirectoryResourceHandle(String name, File file, File codeSource, Manifest manifest) throws MalformedURLException {
        this.name = name;
        this.file = file;
        this.codeSource = codeSource.toURI().toURL();
        this.manifest = manifest;
        url = file.toURI().toURL();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public URL getUrl() {
        return url;
    }

    @Override
    public URL getCodeSourceUrl() {
        return codeSource;
    }

    @Override
    public boolean isDirectory() {
        return file.isDirectory();
    }

    @Override
    public InputStream getInputStream() throws IOException {
        if (file.isDirectory()) {
            return new EmptyInputStream();
        }
        return new FileInputStream(file);
    }

    @Override
    public int getContentLength() {
        if (file.isDirectory() || file.length() > Integer.MAX_VALUE) {
            return -1;
        } else {
            return (int) file.length();
        }
    }

    @Override
    public Manifest getManifest() throws IOException {
        return manifest;
    }

    @Override
    public Attributes getAttributes() throws IOException {
        if (manifest == null) {
            return null;
        }
        return manifest.getAttributes(getName());
    }

    /**
     * Always return null. This could be implementd by verifing the signatures
     * in the manifest file against the actual file, but we don't need this
     * right now.
     *
     * @return null
     */
    public Certificate[] getCertificates() {
        return null;
    }

    static final class EmptyInputStream extends InputStream {

        public int read() {
            return -1;
        }

        public int read(byte b[]) {
            return -1;
        }

        public int read(byte b[], int off, int len) {
            return -1;
        }

        public long skip(long n) {
            return 0;
        }

        public int available() {
            return 0;
        }

        public void close() {
        }

        public synchronized void mark(int readlimit) {
        }

        public synchronized void reset() {
        }

        public boolean markSupported() {
            return false;
        }
    }
}
