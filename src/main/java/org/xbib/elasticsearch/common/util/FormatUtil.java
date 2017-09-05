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

import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.TimeZone;

/**
 * Formatting utilities and constants.
 *
 * The following table describes the tokens
 * used in the pattern language for formatting.
 * <table border="1"><caption>Used tokens</caption>
 * <tr><th>Character</th><th>Duration element</th></tr>
 * <tr><td>y</td><td>years</td></tr>
 * <tr><td>M</td><td>months</td></tr>
 * <tr><td>d</td><td>days</td></tr>
 * <tr><td>H</td><td>hours</td></tr>
 * <tr><td>m</td><td>minutes</td></tr>
 * <tr><td>s</td><td>seconds</td></tr>
 * <tr><td>S</td><td>milliseconds</td></tr>
 * </table>
 *
 * Very similar with https://github.com/apache/commons-lang/blob/master/src/main/java/org/apache/commons/lang3/time/DurationFormatUtils.java
 */
public class FormatUtil {

    /**
     * Number of milliseconds in a standard second.
     */
    public static final long MILLIS_PER_SECOND = 1000;
    /**
     * Number of milliseconds in a standard minute.
     */
    public static final long MILLIS_PER_MINUTE = 60 * MILLIS_PER_SECOND;
    /**
     * Number of milliseconds in a standard hour.
     */
    public static final long MILLIS_PER_HOUR = 60 * MILLIS_PER_MINUTE;
    /**
     * Number of milliseconds in a standard day.
     */
    public static final long MILLIS_PER_DAY = 24 * MILLIS_PER_HOUR;

    /**
     * Pattern used with <code>FastDateFormat</code> and <code>SimpleDateFormat</code>
     * for the ISO8601 period format used in durations.
     *
     * @see java.text.SimpleDateFormat
     */
    public static final String ISO_EXTENDED_FORMAT_PATTERN = "'P'yyyy'Y'M'M'd'DT'H'H'm'M's.S'S'";

    public static String convertFileSize(double size) {
        return convertFileSize(size, Locale.getDefault());
    }

    public static String convertFileSize(double size, Locale locale) {
        String strSize;
        long kb = 1024;
        long mb = 1024 * kb;
        long gb = 1024 * mb;
        long tb = 1024 * gb;

        NumberFormat formatter = NumberFormat.getNumberInstance(locale);
        formatter.setMaximumFractionDigits(2);
        formatter.setMinimumFractionDigits(2);

        if (size < kb) {
            strSize = size + " bytes";
        } else if (size < mb) {
            strSize = formatter.format(size / kb) + " KB";
        } else if (size < gb) {
            strSize = formatter.format(size / mb) + " MB";
        } else if (size < tb) {
            strSize = formatter.format(size / gb) + " GB";
        } else {
            strSize = formatter.format(size / tb) + " TB";
        }
        return strSize;
    }

    /**
     * Formats the time gap as a string.
     *
     * The format used is ISO8601-like:
     * <i>H</i>:<i>m</i>:<i>s</i>.<i>S</i>.
     *
     * @param durationMillis the duration to format
     * @return the time as a String
     */
    public static String formatDurationHMS(long durationMillis) {
        return formatDuration(durationMillis, "H:mm:ss.SSS");
    }

    /**
     * Formats the time gap as a string.
     *
     * The format used is the ISO8601 period format.
     *
     * This method formats durations using the days and lower fields of the
     * ISO format pattern, such as P7D6TH5M4.321S.
     *
     * @param durationMillis the duration to format
     * @return the time as a String
     */
    public static String formatDurationISO(long durationMillis) {
        return formatDuration(durationMillis, ISO_EXTENDED_FORMAT_PATTERN);
    }

    /**
     * Formats the time gap as a string, using the specified format, and
     * using the default timezone.
     *
     * This method formats durations using the days and lower fields of the
     * format pattern. Months and larger are not used.
     *
     * @param durationMillis the duration to format
     * @param format         the way in which to format the duration
     * @return the time as a String
     */
    public static String formatDuration(long durationMillis, String format) {

        Token[] tokens = lexx(format);

        int days = 0;
        int hours = 0;
        int minutes = 0;
        int seconds = 0;
        int milliseconds = 0;

        if (Token.containsTokenWithValue(tokens, d)) {
            days = (int) (durationMillis / MILLIS_PER_DAY);
            durationMillis = durationMillis - (days * MILLIS_PER_DAY);
        }
        if (Token.containsTokenWithValue(tokens, H)) {
            hours = (int) (durationMillis / MILLIS_PER_HOUR);
            durationMillis = durationMillis - (hours * MILLIS_PER_HOUR);
        }
        if (Token.containsTokenWithValue(tokens, m)) {
            minutes = (int) (durationMillis / MILLIS_PER_MINUTE);
            durationMillis = durationMillis - (minutes * MILLIS_PER_MINUTE);
        }
        if (Token.containsTokenWithValue(tokens, s)) {
            seconds = (int) (durationMillis / MILLIS_PER_SECOND);
            durationMillis = durationMillis - (seconds * MILLIS_PER_SECOND);
        }
        if (Token.containsTokenWithValue(tokens, S)) {
            milliseconds = (int) durationMillis;
        }

        return format(tokens, 0, 0, days, hours, minutes, seconds, milliseconds);
    }

    /**
     * Formats an elapsed time into a pluralization correct string.
     *
     * This method formats durations using the days and lower fields of the
     * format pattern. Months and larger are not used.
     *
     * @param durationMillis               the elapsed time to report in milliseconds
     * @param suppressLeadingZeroElements  suppresses leading 0 elements
     * @param suppressTrailingZeroElements suppresses trailing 0 elements
     * @return the formatted text in days/hours/minutes/seconds
     */
    public static String formatDurationWords(
            long durationMillis,
            boolean suppressLeadingZeroElements,
            boolean suppressTrailingZeroElements) {

        // This method is generally replacable by the format method, but 
        // there are a series of tweaks and special cases that require 
        // trickery to replicate.
        String duration = formatDuration(durationMillis, "d' days 'H' hours 'm' minutes 's' seconds'");
        if (suppressLeadingZeroElements) {
            // this is a temporary marker on the front. Like ^ in regexp.
            duration = " " + duration;
            String tmp = Strings.replaceOnce(duration, " 0 days", "");
            if (tmp.length() != duration.length()) {
                duration = tmp;
                tmp = Strings.replaceOnce(duration, " 0 hours", "");
                if (tmp.length() != duration.length()) {
                    duration = tmp;
                    tmp = Strings.replaceOnce(duration, " 0 minutes", "");
                    duration = tmp;
                    if (tmp.length() != duration.length()) {
                        duration = Strings.replaceOnce(tmp, " 0 seconds", "");
                    }
                }
            }
            if (duration.length() != 0) {
                // strip the space off again
                duration = duration.substring(1);
            }
        }
        if (suppressTrailingZeroElements) {
            String tmp = Strings.replaceOnce(duration, " 0 seconds", "");
            if (tmp.length() != duration.length()) {
                duration = tmp;
                tmp = Strings.replaceOnce(duration, " 0 minutes", "");
                if (tmp.length() != duration.length()) {
                    duration = tmp;
                    tmp = Strings.replaceOnce(duration, " 0 hours", "");
                    if (tmp.length() != duration.length()) {
                        duration = Strings.replaceOnce(tmp, " 0 days", "");
                    }
                }
            }
        }
        // handle plurals
        duration = " " + duration;
        duration = Strings.replaceOnce(duration, " 1 seconds", " 1 second");
        duration = Strings.replaceOnce(duration, " 1 minutes", " 1 minute");
        duration = Strings.replaceOnce(duration, " 1 hours", " 1 hour");
        duration = Strings.replaceOnce(duration, " 1 days", " 1 day");
        return duration.trim();
    }

    /**
     * Formats the time gap as a string.
     *
     * The format used is the ISO8601 period format.
     *
     * @param startMillis the start of the duration to format
     * @param endMillis   the end of the duration to format
     * @return the time as a String
     */
    public static String formatPeriodISO(long startMillis, long endMillis) {
        return formatPeriod(startMillis, endMillis, ISO_EXTENDED_FORMAT_PATTERN, TimeZone.getDefault());
    }

    /**
     * <p>Formats the time gap as a string, using the specified format.
     *
     * @param startMillis the start of the duration
     * @param endMillis   the end of the duration
     * @param format      the way in which to format the duration
     * @return the time as a String
     */
    public static String formatPeriod(long startMillis, long endMillis, String format) {
        return formatPeriod(startMillis, endMillis, format, TimeZone.getDefault());
    }

    /**
     * Formats the time gap as a string, using the specified format, and
     * the timezone may be specified.
     *
     * When calculating the difference between months/days, it chooses to
     * calculate months first. So when working out the number of months and
     * days between January 15th and March 10th, it choose 1 month and
     * 23 days gained by choosing January-&gt;February = 1 month and then
     * calculating days forwards, and not the 1 month and 26 days gained by
     * choosing March -&gt; February = 1 month and then calculating days
     * backwards.
     *
     * For more control, the <a href="http://joda-time.sf.net/">Joda-Time</a>
     * library is recommended.
     *
     * @param startMillis the start of the duration
     * @param endMillis   the end of the duration
     * @param format      the way in which to format the duration
     * @param timezone    the millis are defined in
     * @return the time as a String
     */
    public static String formatPeriod(long startMillis, long endMillis, String format,
                                      TimeZone timezone) {

        // Used to optimise for differences under 28 days and 
        // called formatDuration(millis, format); however this did not work 
        // over leap years. 
        // TODO: Compare performance to see if anything was lost by 
        // losing this optimisation. 

        Token[] tokens = lexx(format);

        // timezones get funky around 0, so normalizing everything to GMT 
        // stops the hours being off
        Calendar start = Calendar.getInstance(timezone);
        start.setTime(new Date(startMillis));
        Calendar end = Calendar.getInstance(timezone);
        end.setTime(new Date(endMillis));

        // initial estimates
        int milliseconds = end.get(Calendar.MILLISECOND) - start.get(Calendar.MILLISECOND);
        int seconds = end.get(Calendar.SECOND) - start.get(Calendar.SECOND);
        int minutes = end.get(Calendar.MINUTE) - start.get(Calendar.MINUTE);
        int hours = end.get(Calendar.HOUR_OF_DAY) - start.get(Calendar.HOUR_OF_DAY);
        int days = end.get(Calendar.DAY_OF_MONTH) - start.get(Calendar.DAY_OF_MONTH);
        int months = end.get(Calendar.MONTH) - start.get(Calendar.MONTH);
        int years = end.get(Calendar.YEAR) - start.get(Calendar.YEAR);

        // each initial estimate is adjusted in case it is under 0
        while (milliseconds < 0) {
            milliseconds += 1000;
            seconds -= 1;
        }
        while (seconds < 0) {
            seconds += 60;
            minutes -= 1;
        }
        while (minutes < 0) {
            minutes += 60;
            hours -= 1;
        }
        while (hours < 0) {
            hours += 24;
            days -= 1;
        }

        if (Token.containsTokenWithValue(tokens, M)) {
            while (days < 0) {
                days += start.getActualMaximum(Calendar.DAY_OF_MONTH);
                months -= 1;
                start.add(Calendar.MONTH, 1);
            }

            while (months < 0) {
                months += 12;
                years -= 1;
            }

            if (!Token.containsTokenWithValue(tokens, y) && years != 0) {
                while (years != 0) {
                    months += 12 * years;
                    years = 0;
                }
            }
        } else {
            // there are no M's in the format string

            if (!Token.containsTokenWithValue(tokens, y)) {
                int target = end.get(Calendar.YEAR);
                if (months < 0) {
                    // target is end-year -1
                    target -= 1;
                }

                while ((start.get(Calendar.YEAR) != target)) {
                    days += start.getActualMaximum(Calendar.DAY_OF_YEAR) - start.get(Calendar.DAY_OF_YEAR);

                    // Not sure I grok why this is needed, but the brutal tests show it is
                    if (start instanceof GregorianCalendar) {
                        if ((start.get(Calendar.MONTH) == Calendar.FEBRUARY) &&
                                (start.get(Calendar.DAY_OF_MONTH) == 29)) {
                            days += 1;
                        }
                    }

                    start.add(Calendar.YEAR, 1);

                    days += start.get(Calendar.DAY_OF_YEAR);
                }

                years = 0;
            }

            while (start.get(Calendar.MONTH) != end.get(Calendar.MONTH)) {
                days += start.getActualMaximum(Calendar.DAY_OF_MONTH);
                start.add(Calendar.MONTH, 1);
            }

            months = 0;

            while (days < 0) {
                days += start.getActualMaximum(Calendar.DAY_OF_MONTH);
                months -= 1;
                start.add(Calendar.MONTH, 1);
            }

        }

        // The rest of this code adds in values that 
        // aren't requested. This allows the user to ask for the 
        // number of months and get the real count and not just 0->11.

        if (!Token.containsTokenWithValue(tokens, d)) {
            hours += 24 * days;
            days = 0;
        }
        if (!Token.containsTokenWithValue(tokens, H)) {
            minutes += 60 * hours;
            hours = 0;
        }
        if (!Token.containsTokenWithValue(tokens, m)) {
            seconds += 60 * minutes;
            minutes = 0;
        }
        if (!Token.containsTokenWithValue(tokens, s)) {
            milliseconds += 1000 * seconds;
            seconds = 0;
        }

        return format(tokens, years, months, days, hours, minutes, seconds, milliseconds);
    }

    /**
     * The internal method to do the formatting.
     *
     * @param tokens       the tokens
     * @param years        the number of years
     * @param months       the number of months
     * @param days         the number of days
     * @param hours        the number of hours
     * @param minutes      the number of minutes
     * @param seconds      the number of seconds
     * @param milliseconds the number of millis
     * @return the formatted string
     */
    static String format(Token[] tokens, int years, int months, int days, int hours, int minutes, int seconds,
                         int milliseconds) {
        StringBuilder buffer = new StringBuilder();
        boolean lastOutputSeconds = false;
        int sz = tokens.length;
        for (Token token : tokens) {
            Object value = token.getValue();
            if (value instanceof StringBuilder) {
                buffer.append(value.toString());
            } else {
                if (value == y) {
                    buffer.append(Integer.toString(years));
                    lastOutputSeconds = false;
                } else if (value == M) {
                    buffer.append(Integer
                            .toString(months));
                    lastOutputSeconds = false;
                } else if (value == d) {
                    buffer.append(Integer
                            .toString(days));
                    lastOutputSeconds = false;
                } else if (value == H) {
                    buffer.append(Integer
                            .toString(hours));
                    lastOutputSeconds = false;
                } else if (value == m) {
                    buffer.append(Integer
                            .toString(minutes));
                    lastOutputSeconds = false;
                } else if (value == s) {
                    buffer.append(Integer
                            .toString(seconds));
                    lastOutputSeconds = true;
                } else if (value == S) {
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

    static final Object y = "y";
    static final Object M = "M";
    static final Object d = "d";
    static final Object H = "H";
    static final Object m = "m";
    static final Object s = "s";
    static final Object S = "S";

    /**
     * Parses a classic date format string into Tokens
     *
     * @param format to parse
     * @return array of Token[]
     */
    static Token[] lexx(String format) {
        char[] array = format.toCharArray();
        ArrayList<Token> list = new ArrayList<Token>(array.length);

        boolean inLiteral = false;
        StringBuilder buffer = null;
        Token previous = null;
        int sz = array.length;
        for (char ch : array) {
            if (inLiteral && ch != '\'') {
                buffer.append(ch);
                continue;
            }
            Object value = null;
            switch (ch) {
                case '\'':
                    if (inLiteral) {
                        buffer = null;
                        inLiteral = false;
                    } else {
                        buffer = new StringBuilder();
                        list.add(new Token(buffer));
                        inLiteral = true;
                    }
                    break;
                case 'y':
                    value = y;
                    break;
                case 'M':
                    value = M;
                    break;
                case 'd':
                    value = d;
                    break;
                case 'H':
                    value = H;
                    break;
                case 'm':
                    value = m;
                    break;
                case 's':
                    value = s;
                    break;
                case 'S':
                    value = S;
                    break;
                default:
                    if (buffer == null) {
                        buffer = new StringBuilder();
                        list.add(new Token(buffer));
                    }
                    buffer.append(ch);
            }

            if (value != null) {
                if (previous != null && previous.getValue() == value) {
                    previous.increment();
                } else {
                    Token token = new Token(value);
                    list.add(token);
                    previous = token;
                }
                buffer = null;
            }
        }
        return list.toArray(new Token[list.size()]);
    }

    /**
     * Element that is parsed from the format pattern.
     */
    static class Token {

        /**
         * Helper method to determine if a set of tokens contain a value
         *
         * @param tokens set to look in
         * @param value  to look for
         * @return boolean <code>true</code> if contained
         */
        static boolean containsTokenWithValue(Token[] tokens, Object value) {
            int sz = tokens.length;
            for (Token token : tokens) {
                if (token.getValue() == value) {
                    return true;
                }
            }
            return false;
        }

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
         * Wraps a token around a repeated number of a value, for example it would
         * store 'yyyy' as a value for y and a count of 4.
         *
         * @param value to wrap
         * @param count to wrap
         */
        Token(Object value, int count) {
            this.value = value;
            this.count = count;
        }

        /**
         * Adds another one of the value
         */
        void increment() {
            count++;
        }

        /**
         * Gets the current number of values represented
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
         * @param obj2 Object to consider equality of
         * @return boolean <code>true</code> if equal
         */
        public boolean equals(Object obj2) {
            if (obj2 instanceof Token) {
                Token tok2 = (Token) obj2;
                if (this.value.getClass() != tok2.value.getClass()) {
                    return false;
                }
                if (this.count != tok2.count) {
                    return false;
                }
                if (this.value instanceof StringBuilder) {
                    return this.value.toString().equals(tok2.value.toString());
                } else if (this.value instanceof Number) {
                    return this.value.equals(tok2.value);
                } else {
                    return this.value == tok2.value;
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
        public int hashCode() {
            return this.value.hashCode();
        }

    }

}