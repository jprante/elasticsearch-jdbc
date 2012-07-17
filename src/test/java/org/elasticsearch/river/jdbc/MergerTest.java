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
package org.elasticsearch.river.jdbc;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

public class MergerTest {

    private MockListener mockListener =  new MockListener();


    @Test
    public void test() throws Exception {
        
        String columns[] = new String[] {"_id","person.salary","person.name", "person.position.name", "person.position.since"};
        String row1[] = new String[]{"1","$1000","Joe Doe", "Worker", "2012-06-12"};
        String row2[] = new String[]{"2","$2000","Bill Smith", "Boss", "2012-06-13"};

        Merger merger = new Merger(mockListener, 1L);
        merger.row(columns, row1);
        merger.row(columns, row2);
        merger.close();
    }


    /**
     * Easy test with columns
     * @throws Exception
     */
    @Test
    public void simpleTestWithColumn() throws Exception{

        String columns[] = new String[]{"_optype","_id","label"};
        String row1[] = new String[]{"index","1","label1"};
        String row2[] = new String[]{"index","2","label2"};
        String row3[] = new String[]{"index","3","label3"};
        String row4[] = new String[]{"index","4","label4"};

        Merger merger = new Merger(mockListener,1L);
        merger.row(columns,row1);
        merger.row(columns,row2);
        merger.row(columns,row3);
        merger.row(columns,row4);
        merger.close();
        
        Assert.assertEquals(4,mockListener.getData().size(),"Number of inserted objects");

    }




    private class MockListener extends DefaultAction {
      private Map<String,XContentBuilder> data = new HashMap<String,XContentBuilder>();
      @Override
        public void index(String index, String type, String id, long version, XContentBuilder builder) throws IOException {
           System.err.println("index="+index + " type="+type + " id="+id+ " builder="+builder.string());
            data.put(id,builder);
        }

        @Override
        public void create(String index, String type, String id, long version, XContentBuilder builder) throws IOException {
            data.put(id,builder);
        }

        @Override
        public void delete(String index, String type, String id) throws IOException {
            super.delete(index, type, id);    //To change body of overridden methods use File | Settings | File Templates.
        }

        public Map<String, XContentBuilder> getData() {
            return data;
        }
    };
}
