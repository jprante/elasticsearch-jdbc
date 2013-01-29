/**
 * 
 */
package org.elasticsearch.river.jdbc.strategy.table;

import java.util.Arrays;
import java.util.List;

import org.elasticsearch.river.jdbc.strategy.mock.MockRiverMouth;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author simon00t
 *
 */
public class TableValueTests extends Assert {
	
    @Test
    public void testFilterColumns() throws Exception {
        List<String> columns = Arrays.asList("source_operation", "_id", "label");
        List<String> row1 = Arrays.asList("index", "1", "label1");
        List<String> row2 = Arrays.asList("index", "2", "label2");
        List<String> row3 = Arrays.asList("index", "3", "label3");
        List<String> row4 = Arrays.asList("index", "4", "label4");
        MockRiverMouth target = new MockRiverMouth();
        new TableValueListener()
                .target(target)
                .begin()
                .keys(columns)
                .values(row1)
                .values(row2)
                .values(row3)
                .values(row4)
                .end();
        assertEquals(target.data().size(), 4, "Number of inserted objects");
        assertEquals(target.data().toString(),"{index/null/null/1 {label=\"label1\"}={\"label\":\"label1\"}, " +
        		"index/null/null/2 {label=\"label2\"}={\"label\":\"label2\"}, " +
        				"index/null/null/3 {label=\"label3\"}={\"label\":\"label3\"}, " +
        						"index/null/null/4 {label=\"label4\"}={\"label\":\"label4\"}}");
    }
}
