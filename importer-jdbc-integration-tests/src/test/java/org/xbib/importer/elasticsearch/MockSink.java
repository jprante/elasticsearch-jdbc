package org.xbib.importer.elasticsearch;

import org.xbib.importer.Document;
import org.xbib.importer.Sink;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

/**
 *
 */
public class MockSink implements Sink {

    private final Map<Document,String> data = new TreeMap<>();

    @Override
    public void index(Document document, boolean create) throws IOException {
        data.put(document, document.build());
    }

    @Override
    public void delete(Document document) throws IOException {
        data.remove(document);
    }

    @Override
    public void update(Document document) throws IOException {
        data.remove(document);
        data.put(document, document.build());
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void flush() throws IOException {

    }

    public Map<Document, String> data() {
        return data;
    }
}