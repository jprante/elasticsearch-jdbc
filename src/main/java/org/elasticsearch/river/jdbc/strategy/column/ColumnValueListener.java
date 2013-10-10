/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.river.jdbc.strategy.column;

import java.io.IOException;
import org.elasticsearch.river.jdbc.strategy.simple.SimpleValueListener;
import org.elasticsearch.river.jdbc.support.StructuredObject;

/**
 * Value listener for the 'column' strategy
 * 
 * @author Piotr Śliwa <piotr.sliwa@zineinc.com>
 */
public class ColumnValueListener extends SimpleValueListener<Object> {
    
    private String opType;
    
    public ColumnValueListener(String opType) {
        this.opType = opType;
    }
    
    @Override
    public SimpleValueListener end(StructuredObject object) throws IOException {
        
        if(!object.source().isEmpty()) {
            object.optype(opType);
        }        
        
        return super.end(object);
    }
}
