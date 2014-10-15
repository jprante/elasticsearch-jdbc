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

import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

/**
 * Stub for loading a Java class and execute it. Isolating the main method from other methods is of advantage
 * for JVMs that thoroughly check if the class can be executed.
 */
public class Runner {

    public static void main(String[] args) {
        try {
            // here we load the "concrete" class we want to execute. Add shutdown hook and pass stdin/stdio/stderr.
            Class clazz = Class.forName(args[0]);
            CommandLineInterpreter commandLineInterpreter = (CommandLineInterpreter) clazz.newInstance();
            Runtime.getRuntime().addShutdownHook(commandLineInterpreter.shutdownHook());
            commandLineInterpreter.readFrom(new InputStreamReader(System.in, "UTF-8"))
                    .writeTo(new OutputStreamWriter(System.out, "UTF-8"))
                    .errorsTo(System.err)
                    .start();
        } catch (Throwable e) {
            // ensure fatal errors are printed to stderr
            e.printStackTrace();
            System.exit(1);
        }
        System.exit(0);
    }
}
