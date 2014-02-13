
package org.xbib.io.keyvalue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.List;

/**
 * Read text lines with a key/value pair at each line.
 */
public class KeyValueReader extends BufferedReader
        implements KeyValueStreamListener<String, String> {

    private KeyValueStreamListener<String, String> listener;

    private boolean atStart;

    private boolean atEnd;

    private final char delimiter;

    public KeyValueReader(Reader reader) throws IOException {
        this(reader, '=');
    }
    
    public KeyValueReader(Reader reader, char delimiter) throws IOException {
        super(reader);
        this.atStart = true;
        this.atEnd = false;
        this.delimiter = delimiter;
    }

    public KeyValueReader addListener(KeyValueStreamListener<String, String> listener) {
        this.listener = listener;
        return this;
    }

    @Override
    public int read() throws IOException {
        if (atStart) {
            atStart = false;
            begin();
        }
        return super.read();
    }

    @Override
    public int read(char[] buf, int off, int len) throws IOException {
        if (atStart) {
            atStart = false;
            begin();
        }
        return super.read(buf, off, len);
    }

    @Override
    public String readLine() throws IOException {
        if (atStart) {
            atStart = false;
            begin();
        }
        String line = super.readLine();
        if (line != null) {
            int pos = line.indexOf(delimiter);
            if (pos > 0) {
                keyValue(line.substring(0, pos), line.substring(pos+1));
            }
        }
        return line;
    }

    @Override
    public void close() throws IOException {
        if (!atEnd) {
            atEnd = true;
            end();
        }
        super.close();
    }

    @Override
    public KeyValueReader begin() throws IOException {
        if (listener != null) {
            listener.begin();
        }
        return this;
    }

    @Override
    public KeyValueReader keyValue(String key, String value) throws IOException {
        if (listener != null) {
            listener.keyValue(key, value);
        }
        return this;
    }

    @Override
    public KeyValueReader keyValue(KeyValue<String,String> keyValue) throws IOException {
        if (listener != null) {
            listener.keyValue(keyValue);
        }
        return this;
    }

    @Override
    public KeyValueStreamListener<String, String> keys(List<String> keys) throws IOException {
        if (listener != null) {
            listener.keys(keys);
        }
        return this;
    }

    @Override
    public KeyValueStreamListener<String, String> values(List<String> values) throws IOException {
        if (listener != null) {
            listener.values(values);
        }
        return this;
    }

    @Override
    public KeyValueReader end() throws IOException {
        if (listener != null) {
            listener.end();
        }
        return this;
    }

}
