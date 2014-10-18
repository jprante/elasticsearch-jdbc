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
package org.xbib.elasticsearch.plugin.jdbc.feeder;

import org.xbib.elasticsearch.plugin.jdbc.classloader.uri.URIClassLoader;

import java.io.File;
import java.lang.reflect.Method;
import java.util.Arrays;

/**
 * Stub for loading a Java class and execute it. Isolating the main method from other methods is of advantage
 * for JVMs that thoroughly check if the class can be executed.
 */
public class Runner {

    public static void main(String[] args) {
        try {
            // parse environment for ES_HOME
            String homeFile = System.getenv("ES_HOME");
            if (homeFile == null) {
                System.err.println("Warning: ES_HOME not set, using current directory");
                homeFile = System.getProperty("user.dir");
            }
            // add drivers and Elasticsearch libs from ES_HOME
            ClassLoader cl = getClassLoader(new File(homeFile));
            Thread.currentThread().setContextClassLoader(cl);
            // here we load the "concrete" class we want to execute
            Class clazz = cl.loadClass(args[0]);
            // we can not cast classes, we have different classloaders. Just invoke "exec" method
            Method execMethod = clazz.getMethod("exec");
            execMethod.invoke(clazz.newInstance());
        } catch (Throwable e) {
            // ensure all errors are printed to stderr
            e.printStackTrace();
            System.exit(1);
        }
        System.exit(0);
    }

    private static ClassLoader getClassLoader(File home) {
        URIClassLoader classLoader = new URIClassLoader();
        String[] jars = System.getProperty("java.class.path").split(File.pathSeparator);
        for (String jar : jars) {
            File file = new File(jar);
            // add parent dir if jar
            if (file.getName().toLowerCase().endsWith(".jar")) {
                classLoader.addURI(file.getParentFile().toURI());
            }
            classLoader.addURI(file.toURI());
        }
        // add Elasticsearch jars
        File[] libs = new File(home + "/lib").listFiles();
        if (libs != null) {
            for (File file : libs) {
                if (file.getName().toLowerCase().endsWith(".jar")) {
                    classLoader.addURI(file.toURI());
                }
            }
        }
        // add JDBC plugin jars (if exist)
        libs = new File(home + "/plugins/jdbc").listFiles();
        if (libs != null) {
            for (File file : libs) {
                if (file.getName().toLowerCase().endsWith(".jar")) {
                    classLoader.addURI(file.toURI());
                }
            }
        }
        System.err.println("Classpath: " + Arrays.toString(classLoader.getURIs()));
        return classLoader;
    }

}
