
package org.xbib.elasticsearch.plugin.river.jdbc;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.StringReader;
import java.net.URL;
import java.util.Enumeration;
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
            String pluginName = JDBCRiverPlugin.class.getName();
            Enumeration<URL> e = JDBCRiverPlugin.class.getClassLoader().getResources("es-plugin.properties");
            while (e.hasMoreElements()) {
                URL url = e.nextElement();
                InputStream in = url.openStream();
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                byte[] buffer = new byte[1024];
                int len;
                while ((len = in.read(buffer)) != -1) {
                    out.write(buffer, 0, len);
                }
                in.close();
                Properties props = new Properties();
                props.load(new StringReader(new String(out.toByteArray())));
                String plugin = props.getProperty("plugin");
                if (pluginName.equals(plugin)) {
                    version = props.getProperty("version");
                    hash = props.getProperty("hash");
                    if (!"NA".equals(hash)) {
                        hashShort = hash.substring(0, 7);
                    }
                    timestamp = props.getProperty("timestamp");
                    date = props.getProperty("date");
                }
            }
        } catch (Throwable e) {
            // just ignore...
        }
        INSTANCE = new Build(version, hash, hashShort, timestamp, date);
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
