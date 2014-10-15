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
package org.xbib.elasticsearch.plugin.jdbc.util;

import java.text.NumberFormat;
import java.util.Locale;

public class VolumeFormatUtil {

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
}
