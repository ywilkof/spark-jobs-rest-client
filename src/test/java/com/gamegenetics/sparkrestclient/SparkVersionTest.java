package com.gamegenetics.sparkrestclient;

import org.junit.Test;

import java.util.EnumSet;
import java.util.stream.Stream;

import static org.junit.Assert.*;

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