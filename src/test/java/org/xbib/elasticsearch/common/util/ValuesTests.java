/*
 * Copyright (C) 2015 Jörg Prante
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.xbib.elasticsearch.common.util;

import org.testng.Assert;
import org.testng.annotations.Test;

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