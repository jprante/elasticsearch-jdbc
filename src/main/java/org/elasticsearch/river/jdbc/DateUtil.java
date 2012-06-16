/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.river.jdbc;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

public class DateUtil {

    private static final String ISO_FORMAT_SECONDS = "yyyy-MM-dd'T'HH:mm:ss'Z'";
    private static final SimpleDateFormat isoFormat = new SimpleDateFormat();
    private static final TimeZone tz = TimeZone.getTimeZone("GMT");
    private static final Calendar cal = Calendar.getInstance();

    static {
        isoFormat.applyPattern(ISO_FORMAT_SECONDS);
        isoFormat.setTimeZone(tz);
        isoFormat.setLenient(true);
    }
    
    public static String formatNow() {
        return formatDateISO(new Date());
    }
    
    public synchronized static String formatDateISO(Date date) {
        if (date == null) {
            return null;
        }
        isoFormat.applyPattern(ISO_FORMAT_SECONDS);
        return isoFormat.format(date);
    }

    public synchronized static Date parseDateISO(String value) {
        if (value == null) {
            return null;
        }
        isoFormat.applyPattern(ISO_FORMAT_SECONDS);
        try {
            return isoFormat.parse(value);
        } catch (ParseException pe) {
            // skip
        }
        isoFormat.applyPattern("yyyy-MM-dd");
        try {
            return isoFormat.parse(value);
        } catch (ParseException pe) {
            return null;
        }
    }

    public synchronized static int getYear(Date date) {
        cal.setTime(date);
        return cal.get(Calendar.YEAR);
    }

    public synchronized static Date midnight() {
        return DateUtil.midnight(new Date());
    }

    public synchronized static Date midnight(Date date) {
        cal.setTime(date);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTime();
    }

    public synchronized static Date yesterday() {
        return DateUtil.yesterday(new Date());
    }

    public synchronized static Date yesterday(Date date) {
        return days(date, -1);
    }

    public synchronized static Date tomorrow() {
        return DateUtil.tomorrow(new Date());
    }

    public synchronized static Date tomorrow(Date date) {
        return days(date, 1);
    }

    public synchronized static Date years(int years) {
        return DateUtil.years(new Date(), years);
    }

    public synchronized static Date years(Date date, int years) {
        cal.setTime(date);
        cal.add(Calendar.YEAR, years);
        return cal.getTime();
    }
    
    public synchronized static Date months(int months) {
        return DateUtil.months(new Date(), months);
    }

    public synchronized static Date months(Date date, int months) {
        cal.setTime(date);
        cal.add(Calendar.MONTH, months);
        return cal.getTime();
    }
    
    public static Date weeks(int weeks) {
        return DateUtil.weeks(new Date(), weeks);
    }

    public synchronized static Date weeks(Date date, int weeks) {
        cal.setTime(date);
        cal.add(Calendar.WEEK_OF_YEAR, weeks);
        return cal.getTime();
    }

    public static Date days(int days) {
        return DateUtil.days(new Date(), days);
    }

    public synchronized static Date days(Date date, int days) {
        cal.setTime(date);
        cal.add(Calendar.DAY_OF_YEAR, days);
        return cal.getTime();
    }

    public static Date hours(int hours) {
        return DateUtil.hours(new Date(), hours);
    }

    public synchronized static Date hours(Date date, int hours) {
        cal.setTime(date);
        cal.add(Calendar.HOUR_OF_DAY, hours);
        return cal.getTime();
    }
    
    public static Date minutes(int minutes) {
        return DateUtil.minutes(new Date(), minutes);
    }

    public synchronized static Date minutes(Date date, int minutes) {
        cal.setTime(date);
        cal.add(Calendar.MINUTE, minutes);
        return cal.getTime();
    }
    
    public static Date seconds(int seconds) {
        return DateUtil.seconds(new Date(), seconds);
    }

    public synchronized static Date seconds(Date date, int seconds) {
        cal.setTime(date);
        cal.add(Calendar.MINUTE, seconds);
        return cal.getTime();
    }

}
