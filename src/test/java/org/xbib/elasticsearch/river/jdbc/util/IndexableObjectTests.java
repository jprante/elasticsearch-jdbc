package org.xbib.elasticsearch.river.jdbc.util;

import org.testng.annotations.Test;
import org.xbib.elasticsearch.plugin.jdbc.util.IndexableObject;
import org.xbib.elasticsearch.plugin.jdbc.util.PlainIndexableObject;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

public class IndexableObjectTests {

    @Test
    public void testIndexableObject() {
        PlainIndexableObject a = new PlainIndexableObject();
        PlainIndexableObject b = new PlainIndexableObject();
        assertEquals(a, b);
    }

    @Test
    public void testPlainIndexableObject() {
        IndexableObject a = new PlainIndexableObject()
                .index("index")
                .type("type")
                .id("id");
        IndexableObject b = new PlainIndexableObject()
                .index("index")
                .type("type")
                .id("id");
        assertEquals(a, b);
    }

    @Test
    public void testNotEqualsPlainIndexableObject() {
        IndexableObject a = new PlainIndexableObject()
                .index("index1")
                .type("type1")
                .id("id1");
        IndexableObject b = new PlainIndexableObject()
                .index("index2")
                .type("type2")
                .id("id2");
        assertNotEquals(a, b);
    }

    /**
     * Issue #487
     */
    @Test
    public void testHashCodePlainIndexableObject() {
        IndexableObject a = new PlainIndexableObject()
                .index("my_index")
                .type("Employee")
                .id("12055T");
        IndexableObject b = new PlainIndexableObject()
                .index("my_index")
                .type("Employee")
                .id("120565");
        assertNotEquals(a, b);
    }

}
