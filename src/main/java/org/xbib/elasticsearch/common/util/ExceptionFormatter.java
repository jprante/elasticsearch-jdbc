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
package org.xbib.elasticsearch.common.util;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Method;

/**
 * Format java exception messages and stack traces.
 */
public final class ExceptionFormatter {

    private ExceptionFormatter() {
    }

    @SuppressWarnings("rawtypes")
    public static void append(StringBuilder buf, Throwable t,
                              int level, boolean details) {
        try {
            if (((t != null) && (t.getMessage() != null))
                    && (t.getMessage().length() > 0)) {
                if (details && (level > 0)) {
                    buf.append("\n\nCaused by\n");
                }
                buf.append(t.getMessage());
            }
            if (details) {
                if (t != null) {
                    if ((t.getMessage() != null)
                            && (t.getMessage().length() == 0)) {
                        buf.append("\n\nCaused by ");
                    } else {
                        buf.append("\n\n");
                    }
                }
                StringWriter sw = new StringWriter();
                if (t != null) {
                    t.printStackTrace(new PrintWriter(sw));
                }
                buf.append(sw.toString());
            }
            if (t != null) {
                Method method = t.getClass().getMethod("getCause",
                        new Class[]{});
                Throwable cause = (Throwable) method.invoke(t,
                        (Object) null);
                if (cause != null) {
                    append(buf, cause, level + 1, details);
                }
            }
        } catch (Exception ex) {
            //
        }
    }

    /**
     * Format exception with stack trace
     *
     * @param t the thrown object
     * @return the formatted exception
     */
    public static String format(Throwable t) {
        StringBuilder sb = new StringBuilder();
        append(sb, t, 0, true);
        return sb.toString();
    }
}
