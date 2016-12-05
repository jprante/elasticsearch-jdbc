package org.xbib.importer.util;

import org.testng.annotations.Test;
import org.xbib.importer.elasticsearch.ElasticsearchDocument;
import org.xbib.importer.Document;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

/**
 *
 */
public class DocumentTests {

    @Test
    public void testIndexableObject() {
        ElasticsearchDocument a = new ElasticsearchDocument();
        ElasticsearchDocument b = new ElasticsearchDocument();
        assertEquals(a, b);
    }

    @Test
    public void testPlainIndexableObject() {
        Document a = new ElasticsearchDocument();
        a.setIndex("index");
        a.setType("type");
        a.setId("id");
        Document b = new ElasticsearchDocument();
        b.setIndex("index");
        b.setType("type");
        b.setId("id");
        assertEquals(a, b);
    }

    @Test
    public void testNotEqualsPlainIndexableObject() {
        Document a = new ElasticsearchDocument();
        a.setIndex("index1");
        a.setType("type1");
        a.setId("id1");
        Document b = new ElasticsearchDocument();
        b.setIndex("index2");
        b.setType("type2");
        b.setId("id2");
        assertNotEquals(a, b);
    }

    /**
     * Issue #487
     */
    @Test
    public void testHashCodePlainIndexableObject() {
        Document a = new ElasticsearchDocument();
        a.setIndex("my_index");
        a.setType("Employee");
        a.setId("12055T");
        Document b = new ElasticsearchDocument();
        b.setIndex("my_index");
        b.setType("Employee");
        b.setId("120565");
        assertNotEquals(a, b);
    }

}