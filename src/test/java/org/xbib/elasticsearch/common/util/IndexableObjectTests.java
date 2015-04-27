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

import org.testng.annotations.Test;

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