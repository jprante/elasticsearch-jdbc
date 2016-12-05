package org.xbib.importer.util;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * Test {@link Values}.
 */
public class ValuesTests {

    @Test
    public void testSingleValue() {
        Values<String> vs = new Values<>(null, "TEST", false);
        Object[] values = vs.getValues();
        assertEquals(1, values.length);
        assertEquals("TEST", values[0]);
    }

    @Test
    public void testSingleNull() {
        Values<String> vs = new Values<>(null, null, false);
        Object[] values = vs.getValues();
        assertEquals(values.length, 1);
        assertEquals(values[0], null);
    }

    @Test
    public void testMultipleValues() {
        Values<String> vs = new Values<>(null, "TEST", false);
        vs = new Values<>(vs, "TEST2", false);
        Object[] values = vs.getValues();
        assertEquals(values.length, 2);
        assertEquals(values[0], "TEST");
        assertEquals(values[1], "TEST2");
    }

    @Test
    public void testMultipleValuesNullFirst() {
        Values<String> vs = new Values<>(null, null, false);
        vs = new Values<>(vs, "TEST", false);
        Object[] values = vs.getValues();
        assertEquals(values.length, 1);
        assertEquals(values[0], "TEST");
    }

    @Test
    public void testMultipleValuesNullLast() {
        Values<String> vs = new Values<>(null, "TEST", false);
        vs = new Values<>(vs, null, false);
        Object[] values = vs.getValues();
        assertEquals(values.length, 1);
        assertEquals(values[0], "TEST");
    }

    @Test
    public void testMultipleValuesWithDuplicates() {
        Values<String> vs = new Values<>(null, "TEST", false);
        vs = new Values<>(vs, "TEST2", false);
        vs = new Values<>(vs, "TEST", false);

        Object[] values = vs.getValues();
        assertEquals(values.length, 2);
        assertEquals(values[0], "TEST");
        assertEquals(values[1], "TEST2");
    }

    @Test
    public void testMultipleValuesWithDuplicatesAndNull() {
        Values<String> vs = new Values<>(null, "TEST", false);
        vs = new Values<>(vs, null, false);
        vs = new Values<>(vs, "TEST", false);
        Object[] values = vs.getValues();
        assertEquals(values.length, 1);
        assertEquals(values[0], "TEST");
    }

    @Test
    public void testExpandValue() {
        Values<String> vs = new Values<>(null, "TEST,TEST2", true);
        vs = new Values<>(vs, null, true);
        vs = new Values<>(vs, "TEST3,TEST2", true);
        Object[] values = vs.getValues();
        assertEquals(values.length, 3);
        assertEquals(values[0], "TEST");
        assertEquals(values[1], "TEST2");
        assertEquals(values[2], "TEST3");
    }
}
