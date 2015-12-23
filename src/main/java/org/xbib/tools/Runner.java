/*
 * Copyright (C) 2015 Jörg Prante
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
package org.xbib.tools;

import java.io.FileInputStream;
import java.io.InputStream;

public class Runner {

    public static void main(String[] args) {
        try {
            Class clazz = Class.forName(args[0]);
            CommandLineInterpreter commandLineInterpreter = (CommandLineInterpreter) clazz.newInstance();
            InputStream in = args.length > 1 ? new FileInputStream(args[1]) : System.in;
            commandLineInterpreter.run("args", in);
            in.close();
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(1);
        }
        System.exit(0);
    }

}
