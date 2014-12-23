package org.xbib.cron;

import org.testng.annotations.Test;
import org.xbib.elasticsearch.plugin.jdbc.cron.CronExpression;

import static org.testng.Assert.assertEquals;

public class CronTest {

    @Test
    public void cron() {
        CronExpression expression = new CronExpression("0 0 14-6 ? * FRI-MON");
        assertEquals(expression.getExpressionSummary(),
                "seconds: 0\n" +
                        "minutes: 0\n" +
                        "hours: 0,1,2,3,4,5,6,14,15,16,17,18,19,20,21,22,23\n" +
                        "daysOfMonth: ?\n" +
                        "months: *\n" +
                        "daysOfWeek: 1,2,6,7\n" +
                        "lastdayOfWeek: false\n" +
                        "nearestWeekday: false\n" +
                        "NthDayOfWeek: 0\n" +
                        "lastdayOfMonth: false\n" +
                        "years: *\n"
        );

        expression = new CronExpression("0 0 0-23 ? * *");
        assertEquals(expression.getExpressionSummary(),
                "seconds: 0\n" +
                        "minutes: 0\n" +
                        "hours: 0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23\n" +
                        "daysOfMonth: ?\n" +
                        "months: *\n" +
                        "daysOfWeek: *\n" +
                        "lastdayOfWeek: false\n" +
                        "nearestWeekday: false\n" +
                        "NthDayOfWeek: 0\n" +
                        "lastdayOfMonth: false\n" +
                        "years: *\n"
        );


        expression = new CronExpression("0-59 0-59 0-23 ? * *");
        assertEquals(expression.getExpressionSummary(),
                "seconds: 0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59\n" +
                        "minutes: 0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59\n" +
                        "hours: 0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23\n" +
                        "daysOfMonth: ?\n" +
                        "months: *\n" +
                        "daysOfWeek: *\n" +
                        "lastdayOfWeek: false\n" +
                        "nearestWeekday: false\n" +
                        "NthDayOfWeek: 0\n" +
                        "lastdayOfMonth: false\n" +
                        "years: *\n"
        );

        expression = new CronExpression("0/5 0-59 0-23 ? * *");
        assertEquals(expression.getExpressionSummary(),
                "seconds: 0,5,10,15,20,25,30,35,40,45,50,55\n" +
                        "minutes: 0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59\n" +
                        "hours: 0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23\n" +
                        "daysOfMonth: ?\n" +
                        "months: *\n" +
                        "daysOfWeek: *\n" +
                        "lastdayOfWeek: false\n" +
                        "nearestWeekday: false\n" +
                        "NthDayOfWeek: 0\n" +
                        "lastdayOfMonth: false\n" +
                        "years: *\n"
        );

        // comma test
        expression = new CronExpression("0 5,35 * * * ?");
        assertEquals(expression.getExpressionSummary(),
                "seconds: 0\n" +
                        "minutes: 5,35\n" +
                        "hours: *\n" +
                        "daysOfMonth: *\n" +
                        "months: *\n" +
                        "daysOfWeek: ?\n" +
                        "lastdayOfWeek: false\n" +
                        "nearestWeekday: false\n" +
                        "NthDayOfWeek: 0\n" +
                        "lastdayOfMonth: false\n" +
                        "years: *\n"
        );

        expression = new CronExpression("0/2 * * * * ?");
        assertEquals(expression.getExpressionSummary(),
                "seconds: 0,2,4,6,8,10,12,14,16,18,20,22,24,26,28,30,32,34,36,38,40,42,44,46,48,50,52,54,56,58\n" +
                        "minutes: *\n" +
                        "hours: *\n" +
                        "daysOfMonth: *\n" +
                        "months: *\n" +
                        "daysOfWeek: ?\n" +
                        "lastdayOfWeek: false\n" +
                        "nearestWeekday: false\n" +
                        "NthDayOfWeek: 0\n" +
                        "lastdayOfMonth: false\n" +
                        "years: *\n"
        );
    }
}
