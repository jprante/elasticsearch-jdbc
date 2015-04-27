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

import java.io.IOException;

public class SQLCommandTests extends Assert {
    @Test
    public void simpleQuery() throws IOException {
        SQLCommand sc = new SQLCommand().setSQL("select * from table");
        assertTrue(sc.isQuery());
    }

    @Test
    public void updateQueryType() throws IOException {
        SQLCommand sc = new SQLCommand().setSQL("update foo");
        assertFalse(sc.isQuery());
    }

    @Test
    public void updateWithSubselect() throws IOException {
        SQLCommand sc = new SQLCommand().setSQL("update foo set thingie = select");
        assertFalse(sc.isQuery());
    }

    @Test
    public void updateWithSubselectAndLeadingWhitespace() throws IOException {
        SQLCommand sc = new SQLCommand().setSQL("   update foo set thingie = select");
        assertFalse(sc.isQuery());
    }

    @Test
    public void updateUpperCaseWithSelect() throws IOException {
        SQLCommand sc = new SQLCommand().setSQL("UPDATE foo set thingie = SELECT");
        assertFalse(sc.isQuery());
    }

    @Test
    public void insertWithSelect() throws IOException {
        SQLCommand sc = new SQLCommand().setSQL("insert into foo values select * from bar");
        assertFalse(sc.isQuery());
    }
}
