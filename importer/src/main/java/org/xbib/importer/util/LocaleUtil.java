package org.xbib.importer.util;

import java.util.Locale;

/**
 *
 */
public class LocaleUtil {

    public static Locale toLocale(String localeString) {
        if (localeString == null || localeString.isEmpty()) {
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
                (locale.getCountry() != null && !locale.getCountry().isEmpty() ? "_" + locale.getCountry() : "") +
                (locale.getVariant() != null && !locale.getVariant().isEmpty() ? "_" + locale.getVariant() : "");
    }
}
