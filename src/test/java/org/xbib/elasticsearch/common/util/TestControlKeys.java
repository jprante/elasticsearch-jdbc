package org.xbib.elasticsearch.common.util;

import org.testng.annotations.Test;
import org.xbib.elasticsearch.common.util.ControlKeys;

import java.util.Set;

import static org.testng.Assert.*;

/**
 * Created by sanyu on 2017/9/4.
 */
public class TestControlKeys {
    private final static Set<String> controlKeysFromValues = ControlKeys.makeSetByValues();

    @Test
    public void testMakeSet(){
        assertEquals(controlKeysFromValues.size(), 11);
        assertTrue(controlKeysFromValues.contains("_optype"));

        System.out.println(ControlKeys._optype.hashCode());
        System.out.println(ControlKeys._optype.toString().hashCode());
        System.out.println("_optype".hashCode());
        assertNotEquals(ControlKeys._optype.hashCode(), "_optype".hashCode());
        assertEquals(ControlKeys._optype.toString().hashCode(), "_optype".hashCode());
    }
}
