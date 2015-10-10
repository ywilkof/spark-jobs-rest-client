package com.ywilkof.sparkrestclient;

import org.junit.Test;

/**
 * Created by yonatan on 09.10.15.
 */
public class SparkVersionTest {

    @Test(expected = IllegalArgumentException.class)
    public void testFromVersion_WhenVersionStringNotValidEnumValue_ThenThrowException() throws Exception {
        SparkVersion.fromVersion("");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFromVersion_WhenVersionStringNull_ThenThrowException() throws Exception {
        SparkVersion.fromVersion(null);
    }

}