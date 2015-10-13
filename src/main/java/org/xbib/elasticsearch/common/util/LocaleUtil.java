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

import java.util.Locale;

public class LocaleUtil {

    public static Locale toLocale(String localeString) {
        if (Strings.isNullOrEmpty(localeString)) {
            return Locale.getDefault();
        }
        int separatorCountry = localeString.indexOf('_');
        char separator;
        if (separatorCountry >= 0) {
            separator = '_';
        } else {
            separatorCountry = localeString.indexOf('-');
            separator = '-';
        }
        String language;
        String country;
        String variant;
        if (separatorCountry < 0) {
            language = localeString;
            country = "";
            variant = "";
        } else {
            language = localeString.substring(0, separatorCountry);
            int separatorVariant = localeString.indexOf(separator, separatorCountry + 1);
            if (separatorVariant < 0) {
                country = localeString.substring(separatorCountry + 1);
                variant = "";
            } else {
                country = localeString.substring(separatorCountry + 1, separatorVariant);
                variant = localeString.substring(separatorVariant + 1);
            }
        }
        return new Locale(language, country, variant);
    }

    public static String fromLocale(Locale locale) {
        return locale.getLanguage() +
                (Strings.isNullOrEmpty(locale.getCountry()) ? "_" + locale.getCountry() : "") +
                (Strings.isNullOrEmpty(locale.getVariant()) ? "_" + locale.getVariant() : "");
    }
}
