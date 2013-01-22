package org.elasticsearch.river.jdbc.support;

import org.elasticsearch.river.jdbc.support.ValueSet;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Unit-test checking basic functionality of {@link ValueSet}.
 * @author pdegeus
 */
public class ValueSetTests extends Assert {

    @Test
    public void testSingleValue() {
        ValueSet<String> vs = new ValueSet<String>(null, "TEST");

        Object[] values = vs.getValues();
        assertEquals(1, values.length);
        assertEquals("TEST", values[0]);
    }

    @Test
    public void testSingleNull() {
        ValueSet<String> vs = new ValueSet<String>(null, null);

        Object[] values = vs.getValues();
        assertEquals(values.length, 1);
        assertEquals(values[0], null);
    }

    @Test
    public void testMultipleValues() {
        ValueSet<String> vs = null;
        vs = new ValueSet<String>(vs, "TEST");
        vs = new ValueSet<String>(vs, "TEST2");

        Object[] values = vs.getValues();
        assertEquals(values.length, 2);
        assertEquals(values[0], "TEST");
        assertEquals(values[1], "TEST2");
    }

    @Test
    public void testMultipleValuesNullFirst() {
        ValueSet<String> vs = null;
        vs = new ValueSet<String>(vs, null);
        vs = new ValueSet<String>(vs, "TEST");

        Object[] values = vs.getValues();
        assertEquals(values.length, 1);
        assertEquals(values[0], "TEST");
    }

    @Test
    public void testMultipleValuesNullLast() {
        ValueSet<String> vs = null;
        vs = new ValueSet<String>(vs, "TEST");
        vs = new ValueSet<String>(vs, null);

        Object[] values = vs.getValues();
        assertEquals(values.length, 1);
        assertEquals(values[0], "TEST");
    }

    @Test
    public void testMultipleValuesWithDuplicates() {
        ValueSet<String> vs = null;
        vs = new ValueSet<String>(vs, "TEST");
        vs = new ValueSet<String>(vs, "TEST2");
        vs = new ValueSet<String>(vs, "TEST");

        Object[] values = vs.getValues();
        assertEquals(values.length, 2);
        assertEquals(values[0], "TEST");
        assertEquals(values[1], "TEST2");
    }

    @Test
    public void testMultipleValuesWithDuplicatesAndNull() {
        ValueSet<String> vs = null;
        vs = new ValueSet<String>(vs, "TEST");
        vs = new ValueSet<String>(vs, null);
        vs = new ValueSet<String>(vs, "TEST");

        Object[] values = vs.getValues();
        assertEquals(values.length, 1);
        assertEquals(values[0], "TEST");
    }

}