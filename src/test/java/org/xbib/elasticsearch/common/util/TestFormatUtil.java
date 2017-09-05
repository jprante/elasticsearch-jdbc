package org.xbib.elasticsearch.common.util;

import org.testng.annotations.Test;
import org.xbib.elasticsearch.common.util.FormatUtil;

import static org.testng.Assert.*;


/**
 * Created by neals on 9/4/2017.
 */
public class TestFormatUtil {

    @Test
    public void testLexx() {
        // tests each constant
        assertEquals(new FormatUtil.Token[]{
                new FormatUtil.Token(FormatUtil.y, 1),
                new FormatUtil.Token(FormatUtil.M, 1),
                new FormatUtil.Token(FormatUtil.d, 1),
                new FormatUtil.Token(FormatUtil.H, 1),
                new FormatUtil.Token(FormatUtil.m, 1),
                new FormatUtil.Token(FormatUtil.s, 1),
                new FormatUtil.Token(FormatUtil.S, 1)}, FormatUtil.lexx("yMdHmsS"));

        // tests the ISO 8601-like
        assertEquals(new FormatUtil.Token[]{
                new FormatUtil.Token(FormatUtil.H, 2),
                new FormatUtil.Token(new StringBuilder(":"), 1),
                new FormatUtil.Token(FormatUtil.m, 2),
                new FormatUtil.Token(new StringBuilder(":"), 1),
                new FormatUtil.Token(FormatUtil.s, 2),
                new FormatUtil.Token(new StringBuilder("."), 1),
                new FormatUtil.Token(FormatUtil.S, 3)}, FormatUtil.lexx("HH:mm:ss.SSS"));

        // test the iso extended format
        assertNotEquals(new FormatUtil.Token[]{
                new FormatUtil.Token(new StringBuilder("P"), 1),
                new FormatUtil.Token(FormatUtil.y, 4),
                new FormatUtil.Token(new StringBuilder("Y"), 1),
                new FormatUtil.Token(FormatUtil.M, 1),
                new FormatUtil.Token(new StringBuilder("M"), 1),
                new FormatUtil.Token(FormatUtil.d, 1),
                new FormatUtil.Token(new StringBuilder("DT"), 1),
                new FormatUtil.Token(FormatUtil.H, 1),
                new FormatUtil.Token(new StringBuilder("H"), 1),
                new FormatUtil.Token(FormatUtil.m, 1),
                new FormatUtil.Token(new StringBuilder("M"), 1),
                new FormatUtil.Token(FormatUtil.s, 1),
                new FormatUtil.Token(new StringBuilder("."), 1),
                new FormatUtil.Token(FormatUtil.S, 3),
                new FormatUtil.Token(new StringBuilder("S"), 1)},
                FormatUtil.lexx(FormatUtil.ISO_EXTENDED_FORMAT_PATTERN));

        // test failures in equals
        final FormatUtil.Token token = new FormatUtil.Token(FormatUtil.y, 4);
        assertFalse(token.equals(new Object()), "Token equal to non-Token class. ");
        assertFalse(token.equals(new FormatUtil.Token(new Object())), "Token equal to Token with wrong value class. ");
        assertFalse(token.equals(new FormatUtil.Token(FormatUtil.y, 1)), "Token equal to Token with different count. ");
        final FormatUtil.Token numToken = new FormatUtil.Token(Integer.valueOf(1), 4);
        assertTrue(numToken.equals(numToken), "Token with Number value not equal to itself. ");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testLANG981() { // unmatched quote char in lexx
        FormatUtil.lexx("'yMdHms''S");
    }
}
