
package org.xbib.elasticsearch.plugin.river.jdbc;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringReader;
import java.util.Properties;

public class Build {

    private static final Build INSTANCE;

    static {
        String version = "NA";
        String hash = "NA";
        String hashShort = "NA";
        String timestamp = "NA";
        String date = "NA";

        try {
            InputStream in = Build.class.getResourceAsStream("/es-plugin.properties");
            if (in == null) {
                System.err.println("no es-plugin.properties in class path");
            } else {
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                copy(in, out);
                Properties props = new Properties();
                props.load(new StringReader(new String(out.toByteArray())));
                version = props.getProperty("version");
                hash = props.getProperty("hash");
                if (!"NA".equals(hash)) {
                    hashShort = hash.substring(0, 7);
                }
                timestamp = props.getProperty("timestamp");
                date = props.getProperty("date");
            }
        } catch (Throwable e) {
            // just ignore...
        }
        INSTANCE = new Build(version, hash, hashShort, timestamp, date);
    }

    public static void copy(InputStream in, OutputStream out) throws IOException {
        byte[] buffer = new byte[1024];
        int len;
        while ((len = in.read(buffer)) != -1) {
            out.write(buffer, 0, len);
        }
    }

    private String version;

    private String hash;

    private String hashShort;

    private String timestamp;

    private String date;

    Build(String version, String hash, String hashShort, String timestamp, String date) {
        this.version = version;
        this.hash = hash;
        this.hashShort = hashShort;
        this.timestamp = timestamp;
        this.date = date;
    }

    public static Build getInstance() {
        return INSTANCE;
    }

    public String getVersion() {
        return version;
    }

    public String getHash() {
        return hash;
    }

    public String getShortHash() {
        return hashShort;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public String getDate() {
        return date;
    }
}
