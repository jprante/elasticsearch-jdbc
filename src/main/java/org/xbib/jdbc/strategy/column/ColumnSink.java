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
package org.xbib.jdbc.strategy.column;

import org.xbib.jdbc.strategy.standard.StandardSink;

/**
 * Mouth implementation for the 'column' strategy
 *
 * @author <a href="piotr.sliwa@zineinc.com">Piotr Śliwa</a>
 */
public class ColumnSink extends StandardSink {

    @Override
    public String strategy() {
        return "column";
    }

    @Override
    public ColumnSink newInstance() {
        return new ColumnSink();
    }

}
