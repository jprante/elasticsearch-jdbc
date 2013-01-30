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
package org.elasticsearch.river.jdbc.strategy.table;

import java.io.IOException;
import java.util.Map;

import org.elasticsearch.river.jdbc.strategy.simple.SimpleValueListener;
import org.elasticsearch.river.jdbc.support.StructuredObject;

/**
 * Value listener for the 'table' strategy
 *
 * @author Jörg Prante <joergprante@gmail.com>
 */
public class TableValueListener extends SimpleValueListener {

	public static final String SOURCE_OPERATION = "source_operation";
	public static final String SOURCE_TIMESTAMP = "source_timestamp";
	
	@Override
	protected void map(String k, String v, StructuredObject current)
			throws IOException {
		if(SOURCE_OPERATION.equals(k)) { 
			current.optype(v);
		} else { 
			super.map(k, v, current);
		}
	}

	@Override
	protected Map merge(Map map, String key, Object value) {
		if(SOURCE_OPERATION.equals(key)
				|| SOURCE_TIMESTAMP.equals(key)) { 
			// skip elements in content
			return map;
		} 
		
		return super.merge(map, key, value);
	}
	
	

	
}
