package org.xbib.elasticsearch.river.plugin;

import java.io.IOException;

import org.testng.Assert;
import org.testng.annotations.Test;

import org.xbib.elasticsearch.plugin.jdbc.SQLCommand;

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
