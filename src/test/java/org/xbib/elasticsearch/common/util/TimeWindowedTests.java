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

import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.joda.time.format.DateTimeFormat;
import org.testng.annotations.Test;
import org.xbib.elasticsearch.jdbc.strategy.standard.StandardSink;

import java.io.IOException;

import static org.testng.Assert.assertEquals;

public class TimeWindowedTests {

    @Test
    public void testTimeWindow() throws IOException {
        StandardSink mouth = new StandardSink();
        // daily index format
        String index = "'test-'YYYY.MM.dd";
        mouth.setIndex(index);
        mouth.index(new PlainIndexableObject(), false);
        String dayIndex = DateTimeFormat.forPattern(index).print(new DateTime());
        assertEquals(mouth.getIndex(), dayIndex);
    }
}
