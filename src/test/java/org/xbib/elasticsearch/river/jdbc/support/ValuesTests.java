package org.xbib.elasticsearch.river.jdbc.support;

import org.testng.Assert;
import org.testng.annotations.Test;
import org.xbib.elasticsearch.plugin.jdbc.util.Values;

/**
 * Unit-test checking basic functionality of {@link Values}.
 *
 * @author pdegeus
 */
public class ValuesTests extends Assert {

    @Test
    public void testSingleValue() {
        Values<String> vs = new Values<String>(null, "TEST", false);

        Object[] values = vs.getValues();
        assertEquals(1, values.length);
        assertEquals("TEST", values[0]);
    }

    @Test
    public void testSingleNull() {
        Values<String> vs = new Values<String>(null, null, false);

        Object[] values = vs.getValues();
        assertEquals(values.length, 1);
        assertEquals(values[0], null);
    }

    @Test
    public void testMultipleValues() {
        Values<String> vs = null;
        vs = new Values<String>(vs, "TEST", false);
        vs = new Values<String>(vs, "TEST2", false);

        Object[] values = vs.getValues();
        assertEquals(values.length, 2);
        assertEquals(values[0], "TEST");
        assertEquals(values[1], "TEST2");
    }

    @Test
    public void testMultipleValuesNullFirst() {
        Values<String> vs = null;
        vs = new Values<String>(vs, null, false);
        vs = new Values<String>(vs, "TEST", false);

        Object[] values = vs.getValues();
        assertEquals(values.length, 1);
        assertEquals(values[0], "TEST");
    }

    @Test
    public void testMultipleValuesNullLast() {
        Values<String> vs = null;
        vs = new Values<String>(vs, "TEST", false);
        vs = new Values<String>(vs, null, false);

        Object[] values = vs.getValues();
        assertEquals(values.length, 1);
        assertEquals(values[0], "TEST");
    }

    @Test
    public void testMultipleValuesWithDuplicates() {
        Values<String> vs = null;
        vs = new Values<String>(vs, "TEST", false);
        vs = new Values<String>(vs, "TEST2", false);
        vs = new Values<String>(vs, "TEST", false);

        Object[] values = vs.getValues();
        assertEquals(values.length, 2);
        assertEquals(values[0], "TEST");
        assertEquals(values[1], "TEST2");
    }

    @Test
    public void testMultipleValuesWithDuplicatesAndNull() {
        Values<String> vs = null;
        vs = new Values<String>(vs, "TEST", false);
        vs = new Values<String>(vs, null, false);
        vs = new Values<String>(vs, "TEST", false);

        Object[] values = vs.getValues();
        assertEquals(values.length, 1);
        assertEquals(values[0], "TEST");
    }

    @Test
    public void testExpandValue() {
        Values<String> vs = null;
        vs = new Values<String>(vs, "TEST,TEST2", true);
        vs = new Values<String>(vs, null, true);
        vs = new Values<String>(vs, "TEST3,TEST2", true);

        Object[] values = vs.getValues();
        assertEquals(values.length, 3);
        assertEquals(values[0], "TEST");
        assertEquals(values[1], "TEST2");
        assertEquals(values[2], "TEST3");
    }

}