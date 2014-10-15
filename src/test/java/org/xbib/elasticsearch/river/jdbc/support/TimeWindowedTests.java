package org.xbib.elasticsearch.river.jdbc.support;

import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.joda.time.format.DateTimeFormat;
import org.testng.annotations.Test;
import org.xbib.elasticsearch.plugin.jdbc.util.PlainIndexableObject;
import org.xbib.elasticsearch.river.jdbc.strategy.simple.SimpleRiverMouth;

import java.io.IOException;

import static org.testng.Assert.assertEquals;

public class TimeWindowedTests {

    @Test
    public void testTimeWindow() throws IOException {
        SimpleRiverMouth mouth = new SimpleRiverMouth();
        // daily index format
        String index = "'test-'YYYY.MM.dd";
        mouth.setIndex(index);
        mouth.index(new PlainIndexableObject(), false);
        String dayIndex = DateTimeFormat.forPattern(index).print(new DateTime());
        assertEquals(mouth.getIndex(), dayIndex);
    }
}
