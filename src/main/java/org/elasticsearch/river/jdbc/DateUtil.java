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

import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.joda.time.format.DateTimeFormat;
import org.elasticsearch.common.joda.time.format.ISODateTimeFormat;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

public class DateUtil {

    private static final String ISO_FORMAT_SECONDS = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
    private static final String FORMAT_SECONDS = "yyyy-MM-dd HH:mm:ss";
    //private static final SimpleDateFormat isoFormat = new SimpleDateFormat();
    private static final TimeZone tz = TimeZone.getTimeZone("GMT");

    public static String formatNow() {
        return formatDateISO(new Date());
    }

    public static String formatDateISO(long millis) {
        return new DateTime(millis).toString(ISO_FORMAT_SECONDS);
    }

    public static String formatDateStandard(Date date){
        if (date == null) {
            return null;
        }
        return new DateTime(date).toString(FORMAT_SECONDS);
    }
    
    public synchronized static String formatDateISO(Date date) {
        if (date == null) {
            return null;
        }
        return new DateTime(date).toString(ISO_FORMAT_SECONDS);
    }

    public synchronized static Date parseDateISO(String value) {
        if (value == null) {
            return null;
        }
        try{
            return DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").parseDateTime(value).toDate();
        }catch(Exception e){}

        try{
            return DateTimeFormat.forPattern("yyyy-MM-dd").parseDateTime(value).toDate();
        }   catch (Exception e){
            return null;
        }
    }

    public synchronized static Date parseDate(String value) {
        if (value == null) {
            return null;
        }
        try{
            return DateTimeFormat.forPattern(FORMAT_SECONDS).parseDateTime(value).toDate();
        }catch(Exception e){}

        try{
            return DateTimeFormat.forPattern("yyyy-MM-dd").parseDateTime(value).toDate();
        }   catch (Exception e){
            return null;
        }
    }
}
