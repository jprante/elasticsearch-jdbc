package org.xbib.importer.util;

import org.xbib.time.pretty.PrettyTime;

import java.util.ArrayList;
import java.util.List;

/**
 * Taken from org,apache.commons.lang.DurationFormatUtils of Apache commons-lang.
 */
public class FormatUtil {

    private static final PrettyTime pretty = new PrettyTime();

    private static final String EMPTY = "";
    private static final String YEAR = "y";
    private static final String MONTH = "M";
    private static final String DAY = "d";
    private static final String HOUR = "H";
    private static final String MINUTE = "m";
    private static final String SECOND = "s";
    private static final String MILLISECOND = "S";

    /**
     * Number of milliseconds in a standard second.
     */
    private static final long MILLIS_PER_SECOND = 1000;
    /**
     * Number of milliseconds in a standard minute.
     */
    private static final long MILLIS_PER_MINUTE = 60 * MILLIS_PER_SECOND;
    /**
     * Number of milliseconds in a standard hour.
     */
    private static final long MILLIS_PER_HOUR = 60 * MILLIS_PER_MINUTE;
    /**
     * Number of milliseconds in a standard day.
     */
    private static final long MILLIS_PER_DAY = 24 * MILLIS_PER_HOUR;

    private static final String[] BYTES = {
            " B", " kB", " MB", " GB", " TB", " PB", " EB", " ZB", " YB"
    };
    private static final String[] BYTES_PER_SECOND = {
            " B/s", " kB/s", " MB/s", " GB/s", " TB/s", " PB/s", " EB/s", " ZB/s", " YB/s"
    };
    private static final String[] DOCS_PER_SECOND = {
            " dps", " kdps", " Mdps", " Gdps", " Tdps", " Pdps", " Edps", " Zdps", " Ydps"
    };


    /**
     * Format byte size (file size as example) into a string,
     * with two digits after dot and actual measure (MB, GB or other).
     *
     * @param size value to format
     * @return formatted string in bytes, kB, MB or other.
     */
    public static String formatSize(long size) {
        return format(size, BYTES, 1024);
    }

    public static String formatSize(double size) {
        return format(size, BYTES, 1024);
    }

    /**
     * Format speed values (copy speed as example) into a string
     * with two digits after dot and actual measure (MB/s, GB/s or other).
     *
     * @param speed value to format
     * @return formatted string in bytes/s, kB/s, MB/s or other.
     */
    public static String formatSpeed(long speed) {
        return format(speed, BYTES_PER_SECOND, 1024);
    }

    public static String formatSpeed(double speed) {
        return format(speed, BYTES_PER_SECOND, 1024);
    }

    public static String formatDocumentSpeed(long speed) {
        return format(speed, DOCS_PER_SECOND, 1024);
    }

    public static String formatDocumentSpeed(double speed) {
        return format(speed, DOCS_PER_SECOND, 1024);
    }

    /**
     * Format any value without string appending.
     *
     * @param size            value to format
     * @param measureUnits    array of strings to use as measurement units. Use BYTES_PER_SECOND as example.
     * @param measureQuantity quantiry, required to step into next unit. Like 1024 for bytes, 1000 for meters or 100 for centiry.
     * @return formatted size with measure unit
     */
    private static String format(long size, String[] measureUnits, int measureQuantity) {
        if (size <= 0) {
            return null;
        }
        if (size < measureQuantity) {
            return size + measureUnits[0];
        }
        int i = 1;
        double d = size;
        while ((d = d / measureQuantity) > (measureQuantity - 1)) {
            i++;
        }
        long l = (long) (d * 100);
        d = (double) l / 100;
        if (i < measureUnits.length) {
            return d + measureUnits[i];
        }
        return String.valueOf(size);
    }

    private static String format(double value, String[] measureUnits, int measureQuantity) {
        double d = value;
        if (d <= 0.0d) {
            return null;
        }
        if (d < measureQuantity) {
            return d + measureUnits[0];
        }
        int i = 1;
        while ((d = d / measureQuantity) > (measureQuantity - 1)) {
            i++;
        }
        long l = (long) (d * 100);
        d = (double) l / 100;
        if (i < measureUnits.length) {
            return d + measureUnits[i];
        }
        return String.valueOf(d);
    }

    public static String formatMillis(long millis) {
        return pretty.format(pretty.calculateDuration(millis));
    }

    public static String formatDurationWords(long value, boolean suppressLeadingZeroElements,
            boolean suppressTrailingZeroElements) {
        // This method is generally replacable by the format method, but
        // there are a series of tweaks and special cases that require
        // trickery to replicate.
        String duration = formatDuration(value, "d' days 'H' hours 'm' minutes 's' seconds'");
        if (suppressLeadingZeroElements) {
            // this is a temporary marker on the front. Like ^ in regexp.
            duration = " " + duration;
            String tmp = replaceOnce(duration, " 0 days", "");
            if (tmp.length() != duration.length()) {
                duration = tmp;
                tmp = replaceOnce(duration, " 0 hours", "");
                if (tmp.length() != duration.length()) {
                    duration = tmp;
                    tmp = replaceOnce(duration, " 0 minutes", "");
                    duration = tmp;
                    if (tmp.length() != duration.length()) {
                        duration = replaceOnce(tmp, " 0 seconds", "");
                    }
                }
            }
            if (duration.length() != 0) {
                // strip the space off again
                duration = duration.substring(1);
            }
        }
        if (suppressTrailingZeroElements) {
            String tmp = replaceOnce(duration, " 0 seconds", "");
            if (tmp.length() != duration.length()) {
                duration = tmp;
                tmp = replaceOnce(duration, " 0 minutes", "");
                if (tmp.length() != duration.length()) {
                    duration = tmp;
                    tmp = replaceOnce(duration, " 0 hours", "");
                    if (tmp.length() != duration.length()) {
                        duration = replaceOnce(tmp, " 0 days", "");
                    }
                }
            }
        }
        // handle plurals
        duration = " " + duration;
        duration = replaceOnce(duration, " 1 seconds", " 1 second");
        duration = replaceOnce(duration, " 1 minutes", " 1 minute");
        duration = replaceOnce(duration, " 1 hours", " 1 hour");
        duration = replaceOnce(duration, " 1 days", " 1 day");
        return duration.trim();
    }

    public static String formatDuration(long millis, String format) {
        long durationMillis = millis;
        List<Token> tokens = lexx(format);
        int days = 0;
        int hours = 0;
        int minutes = 0;
        int seconds = 0;
        int milliseconds = 0;

        if (Token.containsTokenWithValue(tokens, DAY)) {
            days = (int) (durationMillis / MILLIS_PER_DAY);
            durationMillis -= days * MILLIS_PER_DAY;
        }
        if (Token.containsTokenWithValue(tokens, HOUR)) {
            hours = (int) (durationMillis / MILLIS_PER_HOUR);
            durationMillis -= hours * MILLIS_PER_HOUR;
        }
        if (Token.containsTokenWithValue(tokens, MINUTE)) {
            minutes = (int) (durationMillis / MILLIS_PER_MINUTE);
            durationMillis -= minutes * MILLIS_PER_MINUTE;
        }
        if (Token.containsTokenWithValue(tokens, SECOND)) {
            seconds = (int) (durationMillis / MILLIS_PER_SECOND);
            durationMillis -= seconds * MILLIS_PER_SECOND;
        }
        if (Token.containsTokenWithValue(tokens, MILLISECOND)) {
            milliseconds = (int) durationMillis;
        }
        return format(tokens, 0, 0, days, hours, minutes, seconds, milliseconds);
    }

    /**
     * <p>The internal method to do the formatting.</p>
     *
     * @param tokens       the tokens
     * @param years        the number of years
     * @param months       the number of months
     * @param days         the number of days
     * @param hours        the number of hours
     * @param minutes      the number of minutes
     * @param seconds      the number of seconds
     * @param millis the number of millis
     * @return the formatted string
     */
    private static String format(List<Token> tokens,
                                 int years, int months, int days, int hours, int minutes, int seconds, int millis) {
        int milliseconds = millis;
        StringBuilder buffer = new StringBuilder();
        boolean lastOutputSeconds = false;
        for (Token token : tokens) {
            Object value = token.getValue();
            if (value instanceof StringBuilder) {
                buffer.append(value.toString());
            } else {
                if (YEAR.equals(value)) {
                    buffer.append(Integer.toString(years));
                    lastOutputSeconds = false;
                } else if (MONTH.equals(value)) {
                    buffer.append(Integer.toString(months));
                    lastOutputSeconds = false;
                } else if (DAY.equals(value)) {
                    buffer.append(Integer.toString(days));
                    lastOutputSeconds = false;
                } else if (HOUR.equals(value)) {
                    buffer.append(Integer.toString(hours));
                    lastOutputSeconds = false;
                } else if (MINUTE.equals(value)) {
                    buffer.append(Integer.toString(minutes));
                    lastOutputSeconds = false;
                } else if (SECOND.equals(value)) {
                    buffer.append(Integer.toString(seconds));
                    lastOutputSeconds = true;
                } else if (MILLISECOND.equals(value)) {
                    if (lastOutputSeconds) {
                        milliseconds += 1000;
                        String str = Integer.toString(milliseconds);
                        buffer.append(str.substring(1));
                    } else {
                        buffer.append(Integer.toString(milliseconds));
                    }
                    lastOutputSeconds = false;
                }
            }
        }
        return buffer.toString();
    }

    /**
     * Parses a classic date format string into Tokens.
     *
     * @param format to parse
     * @return array of Token[]
     */
    private static List<Token> lexx(String format) {
        char[] array = format.toCharArray();
        List<Token> list = new ArrayList<>(array.length);
        boolean inLiteral = false;
        StringBuilder sb = new StringBuilder();
        Token previous = null;
        for (char ch : array) {
            if (inLiteral && ch != '\'') {
                sb.append(ch);
                continue;
            }
            Object value = null;
            switch (ch) {
                case '\'':
                    if (inLiteral) {
                        sb = new StringBuilder();
                        inLiteral = false;
                    } else {
                        sb = new StringBuilder();
                        list.add(new Token(sb));
                        inLiteral = true;
                    }
                    break;
                case 'y':
                    value = YEAR;
                    break;
                case 'M':
                    value = MONTH;
                    break;
                case 'd':
                    value = DAY;
                    break;
                case 'H':
                    value = HOUR;
                    break;
                case 'm':
                    value = MINUTE;
                    break;
                case 's':
                    value = SECOND;
                    break;
                case 'S':
                    value = MILLISECOND;
                    break;
                default:
                    if (sb.length() == 0) {
                        sb = new StringBuilder();
                        list.add(new Token(sb));
                    }
                    sb.append(ch);
            }
            if (value != null) {
                if (previous != null && value.equals(previous.getValue())) {
                    previous.increment();
                } else {
                    Token token = new Token(value);
                    list.add(token);
                    previous = token;
                }
                sb.setLength(0);
            }
        }
        return list;
    }

    private static String replaceOnce(String text, String searchString, String replacement) {
        return replace(text, searchString, replacement, 1);
    }

    private static String replace(String text, String searchString, String replacement, int maxvalue) {
        int max = maxvalue;
        if (isNullOrEmpty(text) || isNullOrEmpty(searchString) || replacement == null || max == 0) {
            return text;
        }
        int start = 0;
        int end = text.indexOf(searchString, start);
        if (end == -1) {
            return text;
        }
        int replLength = searchString.length();
        int increase = replacement.length() - replLength;
        increase = (increase < 0 ? 0 : increase);
        increase *= (max < 0 ? 16 : (max > 64 ? 64 : max));
        StringBuilder buf = new StringBuilder(text.length() + increase);
        while (end != -1) {
            buf.append(text.substring(start, end)).append(replacement);
            start = end + replLength;
            if (--max == 0) {
                break;
            }
            end = text.indexOf(searchString, start);
        }
        buf.append(text.substring(start));
        return buf.toString();
    }

    private static boolean isNullOrEmpty(String target) {
        return target == null || EMPTY.equals(target);
    }


    /**
     * Element that is parsed from the format pattern.
     */
    private static class Token {

        private Object value;
        private int count;

        /**
         * Wraps a token around a value. A value would be something like a 'Y'.
         *
         * @param value to wrap
         */
        Token(Object value) {
            this.value = value;
            this.count = 1;
        }

        /**
         * Helper method to determine if a set of tokens contain a value.
         *
         * @param tokens set to look in
         * @param value  to look for
         * @return boolean <code>true</code> if contained
         */
        static boolean containsTokenWithValue(List<Token> tokens, Object value) {
            for (Token token : tokens) {
                if (token.getValue().equals(value)) {
                    return true;
                }
            }
            return false;
        }

        /**
         * Adds another one of the value.
         */
        void increment() {
            count++;
        }

        /**
         * Gets the current number of values represented.
         *
         * @return int number of values represented
         */
        int getCount() {
            return count;
        }

        /**
         * Gets the particular value this token represents.
         *
         * @return Object value
         */
        Object getValue() {
            return value;
        }

        /**
         * Supports equality of this Token to another Token.
         *
         * @param obj Object to consider equality of
         * @return boolean <code>true</code> if equal
         */
        @Override
        public boolean equals(Object obj) {
            if (obj instanceof Token) {
                Token tok = (Token) obj;
                if (this.value.getClass() != tok.value.getClass()) {
                    return false;
                }
                if (this.count != tok.count) {
                    return false;
                }
                if (this.value instanceof StringBuilder) {
                    return this.value.toString().equals(tok.value.toString());
                } else if (this.value instanceof Number) {
                    return this.value.equals(tok.value);
                } else {
                    return this.value == tok.value;
                }
            }
            return false;
        }

        /**
         * Returns a hashcode for the token equal to the
         * hashcode for the token's value. Thus 'TT' and 'TTTT'
         * will have the same hashcode.
         *
         * @return The hashcode for the token
         */
        @Override
        public int hashCode() {
            return this.value.hashCode();
        }

        @Override
        public String toString() {
            return value + " (" + count + ")";
        }
    }
}
