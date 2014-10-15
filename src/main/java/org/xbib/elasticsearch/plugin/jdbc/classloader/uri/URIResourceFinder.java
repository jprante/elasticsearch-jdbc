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
package org.xbib.elasticsearch.plugin.jdbc.classloader.uri;

import org.xbib.elasticsearch.plugin.jdbc.classloader.ResourceEnumeration;
import org.xbib.elasticsearch.plugin.jdbc.classloader.ResourceFinder;
import org.xbib.elasticsearch.plugin.jdbc.classloader.ResourceHandle;
import org.xbib.elasticsearch.plugin.jdbc.classloader.ResourceLocation;
import org.xbib.elasticsearch.plugin.jdbc.classloader.directory.DirectoryResourceLocation;
import org.xbib.elasticsearch.plugin.jdbc.classloader.jar.JarResourceLocation;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

public class URIResourceFinder implements ResourceFinder {

    private final Object lock = new Object();

    private final Set<URI> uris = new LinkedHashSet<URI>();

    private final Map<URI, ResourceLocation> classPath = new LinkedHashMap<URI, ResourceLocation>();

    private final Set<File> watchedFiles = new LinkedHashSet<File>();

    private boolean destroyed = false;

    public URIResourceFinder() {
    }

    public void destroy() {
        synchronized (lock) {
            if (destroyed) {
                return;
            }
            destroyed = true;
            uris.clear();
            for (ResourceLocation resourceLocation : classPath.values()) {
                resourceLocation.close();
            }
            classPath.clear();
        }
    }

    public ResourceHandle getResource(String resourceName) {
        synchronized (lock) {
            if (destroyed) {
                return null;
            }
            Map<URI, ResourceLocation> path = getClassPath();
            for (Map.Entry<URI, ResourceLocation> entry : path.entrySet()) {
                ResourceLocation resourceLocation = entry.getValue();
                ResourceHandle resourceHandle = resourceLocation.getResourceHandle(resourceName);
                if (resourceHandle != null && !resourceHandle.isDirectory()) {
                    return resourceHandle;
                }
            }
        }
        return null;
    }

    public URL findResource(String resourceName) {
        synchronized (lock) {
            if (destroyed) {
                return null;
            }
            for (Map.Entry<URI, ResourceLocation> entry : getClassPath().entrySet()) {
                ResourceLocation resourceLocation = entry.getValue();
                ResourceHandle resourceHandle = resourceLocation.getResourceHandle(resourceName);
                if (resourceHandle != null) {
                    return resourceHandle.getUrl();
                }
            }
        }
        return null;
    }

    public Enumeration<URL> findResources(String resourceName) {
        synchronized (lock) {
            return new ResourceEnumeration(new ArrayList<ResourceLocation>(getClassPath().values()), resourceName);
        }
    }

    public void addURI(URI uri) {
        add(Arrays.asList(uri));
    }

    public URI[] getURIs() {
        synchronized (lock) {
            return uris.toArray(new URI[uris.size()]);
        }
    }

    /**
     * Adds a list of uris to the end of this class loader.
     *
     * @param uris the URLs to add
     */
    protected void add(List<URI> uris) {
        synchronized (lock) {
            if (destroyed) {
                throw new IllegalStateException("UriResourceFinder has been destroyed");
            }
            boolean shouldRebuild = this.uris.addAll(uris);
            if (shouldRebuild) {
                rebuildClassPath();
            }
        }
    }

    private Map<URI, ResourceLocation> getClassPath() {
        assert Thread.holdsLock(lock) : "This method can only be called while holding the lock";
        for (File file : watchedFiles) {
            if (file.canRead()) {
                rebuildClassPath();
                break;
            }
        }
        return classPath;
    }

    /**
     * Rebuilds the entire class path. This class is called when new URIs are
     * added or one of the watched files becomes readable. This method will not
     * open jar files again, but will add any new entries not alredy open to the
     * class path. If any file based uri is does not exist, we will watch for
     * that file to appear.
     */
    private void rebuildClassPath() {
        assert Thread.holdsLock(lock) : "This method can only be called while holding the lock";
        // copy all of the existing locations into a temp map and clear the class path
        Map<URI, ResourceLocation> existingJarFiles = new LinkedHashMap<URI, ResourceLocation>(classPath);
        classPath.clear();
        LinkedList<URI> locationStack = new LinkedList<URI>(uris);
        try {
            while (!locationStack.isEmpty()) {
                URI uri = locationStack.removeFirst();
                if (classPath.containsKey(uri)) {
                    continue;
                }
                // Check is this URL has already been opened
                ResourceLocation resourceLocation = existingJarFiles.remove(uri);
                // If not opened, cache the uri and wrap it with a resource location
                if (resourceLocation == null) {
                    try {
                        resourceLocation = createResourceLocation(uri.toURL(), cacheUri(uri));
                    } catch (FileNotFoundException e) {
                        // if this is a file URL, the file doesn't exist yet... watch to see if it appears later
                        if ("file".equals(uri.getScheme())) {
                            File file = new File(uri.getPath());
                            watchedFiles.add(file);
                            continue;

                        }
                    } catch (IOException ignored) {
                        // can't seem to open the file... this is most likely a bad jar file
                        // so don't keep a watch out for it because that would require lots of checking
                        // Dain: We may want to review this decision later
                        continue;
                    } catch (UnsupportedOperationException ex) {
                        // the protocol for the JAR file's URL is not supported.  This can occur when
                        // the jar file is embedded in an EAR or CAR file.
                        continue;
                    }
                }
                try {
                    // add the jar to our class path
                    if (resourceLocation != null && resourceLocation.getCodeSource() != null) {
                        classPath.put(resourceLocation.getCodeSource().toURI(), resourceLocation);
                    }
                } catch (URISyntaxException ex) {
                    // ignore
                }
                // push the manifest classpath on the stack (make sure to maintain the order)
                List<URI> manifestClassPath = getManifestClassPath(resourceLocation);
                locationStack.addAll(0, manifestClassPath);
            }
        } catch (Error e) {
            destroy();
            throw e;
        }
        for (ResourceLocation resourceLocation : existingJarFiles.values()) {
            resourceLocation.close();
        }
    }

    protected File cacheUri(URI uri) throws IOException {
        if (!"file".equals(uri.getScheme())) {
            // download the jar
            throw new UnsupportedOperationException("Only local file jars are supported " + uri);
        }
        File file = new File(uri.getPath());
        if (!file.exists()) {
            throw new FileNotFoundException(file.getAbsolutePath());
        }
        if (!file.canRead()) {
            throw new IOException("File is not readable: " + file.getAbsolutePath());
        }
        return file;
    }

    protected ResourceLocation createResourceLocation(URL codeSource, File cacheFile) throws IOException {
        if (!cacheFile.exists()) {
            throw new FileNotFoundException(cacheFile.getAbsolutePath());
        }
        if (!cacheFile.canRead()) {
            throw new IOException("File is not readable: " + cacheFile.getAbsolutePath());
        }
        return cacheFile.isDirectory() ?
                // DirectoryResourceLocation will only return "file" URLs within this directory
                // do not use the DirectoryResourceLocation for non file based uris
                new DirectoryResourceLocation(cacheFile) :
                new JarResourceLocation(codeSource, cacheFile);
    }

    private List<URI> getManifestClassPath(ResourceLocation resourceLocation) {
        List<URI> classPathUrls = new LinkedList<URI>();
        try {
            // get the manifest, if possible
            Manifest manifest = resourceLocation.getManifest();
            if (manifest == null) {
                // some locations don't have a manifest
                return classPathUrls;
            }
            // get the class-path attribute, if possible
            String manifestClassPath = manifest.getMainAttributes().getValue(Attributes.Name.CLASS_PATH);
            if (manifestClassPath == null) {
                return classPathUrls;
            }
            // build the uris...
            // the class-path attribute is space delimited
            URL codeSource = resourceLocation.getCodeSource();
            for (StringTokenizer tokenizer = new StringTokenizer(manifestClassPath, " "); tokenizer.hasMoreTokens(); ) {
                String entry = tokenizer.nextToken();
                try {
                    // the class path entry is relative to the resource location code source
                    URL entryUrl = new URL(codeSource, entry);
                    classPathUrls.add(entryUrl.toURI());
                } catch (MalformedURLException ignored) {
                    // most likely a poorly named entry
                } catch (URISyntaxException ignored) {
                    // most likely a poorly named entry
                }
            }
            return classPathUrls;
        } catch (IOException ignored) {
            // error opening the manifest
            return classPathUrls;
        }
    }
}
