package org.xbib.importer.util;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * Format java exception messages and stack traces.
 */
public final class ExceptionFormatter {

    private ExceptionFormatter() {
    }

    /**
     * Format exception with stack trace.
     *
     * @param t the thrown object
     * @return the formatted exception
     */
    public static String format(Throwable t) {
        StringBuilder sb = new StringBuilder();
        append(sb, t, 0, true);
        return sb.toString();
    }

    /**
     * Append Exception to string builder.
     */
    private static void append(StringBuilder sb, Throwable t, int level, boolean details) {
        if (((t != null) && (t.getMessage() != null)) && (!t.getMessage().isEmpty())) {
            if (details && (level > 0)) {
                sb.append("\n\nCaused by\n");
            }
            sb.append(t.getMessage());
        }
        if (details) {
            if (t != null) {
                if ((t.getMessage() != null) && (t.getMessage().isEmpty())) {
                    sb.append("\n\nCaused by ");
                } else {
                    sb.append("\n\n");
                }
            }
            StringWriter sw = new StringWriter();
            if (t != null) {
                t.printStackTrace(new PrintWriter(sw));
            }
            sb.append(sw.toString());
        }
        if (t != null && t.getCause() != null) {
            append(sb, t.getCause(), level + 1, details);
        }
    }
}
