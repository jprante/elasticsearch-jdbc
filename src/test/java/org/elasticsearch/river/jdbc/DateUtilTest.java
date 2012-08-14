package org.elasticsearch.river.jdbc;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Date;

public class DateUtilTest {


    @Test
    public void testDate(){
        String datestr = "2012-05-01 12:00:02";

        Date date = DateUtil.parseDate(datestr);

        Assert.assertEquals(DateUtil.formatDateStandard(date),datestr);
    }
}
