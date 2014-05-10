/*
 * Licensed to Jörg Prante and xbib under one or more contributor 
 * license agreements. See the NOTICE.txt file distributed with this work
 * for additional information regarding copyright ownership.
 *
 * Copyright (C) 2012 Jörg Prante and xbib
 * 
 * This program is free software; you can redistribute it and/or modify 
 * it under the terms of the GNU Affero General Public License as published 
 * by the Free Software Foundation; either version 3 of the License, or 
 * (at your option) any later version.
 * This program is distributed in the hope that it will be useful, 
 * but WITHOUT ANY WARRANTY; without even the implied warranty of 
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the 
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License 
 * along with this program; if not, see http://www.gnu.org/licenses 
 * or write to the Free Software Foundation, Inc., 51 Franklin Street, 
 * Fifth Floor, Boston, MA 02110-1301 USA.
 * 
 * The interactive user interfaces in modified source and object code 
 * versions of this program must display Appropriate Legal Notices, 
 * as required under Section 5 of the GNU Affero General Public License.
 * 
 * In accordance with Section 7(b) of the GNU Affero General Public 
 * License, these Appropriate Legal Notices must retain the display of the 
 * "Powered by xbib" logo. If the display of the logo is not reasonably 
 * feasible for technical reasons, the Appropriate Legal Notices must display
 * the words "Powered by xbib".
 */
package org.xbib.keyvalue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.List;

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
