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
package org.xbib.elasticsearch.jdbc.strategy.standard;

import org.testng.annotations.Parameters;
import org.testng.annotations.Test;
import org.xbib.elasticsearch.jdbc.strategy.Context;
import org.xbib.elasticsearch.jdbc.strategy.JDBCSource;

public class StandardScriptTests extends AbstractSinkTest {

    @Override
    public JDBCSource newSource() {
        return new StandardSource();
    }

    @Override
    public StandardContext newContext() {
        return new StandardContext();
    }

    /**
     * Orders table (star query)
     *
     * @param resource the definition
     * @throws Exception if test fails
     */
    @Test
    @Parameters({"task1"})
    public void testSimpleTaskOnce(String resource) throws Exception {
        Context context = createContext(resource);
        boolean b = waitFor(context, Context.State.IDLE, 5000L);
        logger.info("after wait for: {}", b);
    }

}
