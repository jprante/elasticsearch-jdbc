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

import org.xbib.elasticsearch.plugin.jdbc.classloader.AbstractURLResourceLocation;
import org.xbib.elasticsearch.plugin.jdbc.classloader.ResourceHandle;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.jar.Manifest;

public class DirectoryResourceLocation extends AbstractURLResourceLocation {

    private final File baseDir;

    private boolean manifestLoaded = false;

    private Manifest manifest;

    public DirectoryResourceLocation(File baseDir) throws MalformedURLException {
        super(baseDir.toURI().toURL());
        this.baseDir = baseDir;
    }

    @Override
    public ResourceHandle getResourceHandle(String resourceName) {
        File file = new File(baseDir, resourceName);
        if (!file.exists() || !isLocal(file)) {
            return null;
        }
        try {
            return new DirectoryResourceHandle(resourceName, file, baseDir, getManifestSafe());
        } catch (MalformedURLException e) {
            return null;
        }
    }

    private boolean isLocal(File file) {
        try {
            String base = baseDir.getCanonicalPath();
            String relative = file.getCanonicalPath();
            return (relative.startsWith(base));
        } catch (IOException e) {
            return false;
        }
    }

    @Override
    public Manifest getManifest() throws IOException {
        if (!manifestLoaded) {
            File manifestFile = new File(baseDir, "META-INF/MANIFEST.MF");
            if (manifestFile.isFile() && manifestFile.canRead()) {
                FileInputStream in = null;
                try {
                    in = new FileInputStream(manifestFile);
                    manifest = new Manifest(in);
                } finally {
                    if (in != null) {
                        try {
                            in.close();
                        } catch (Exception ignored) {
                        }
                    }
                }
            }
            manifestLoaded = true;
        }
        return manifest;
    }

    private Manifest getManifestSafe() {
        Manifest m = null;
        try {
            m = getManifest();
        } catch (IOException e) {
            // ignore
        }
        return m;
    }
}
