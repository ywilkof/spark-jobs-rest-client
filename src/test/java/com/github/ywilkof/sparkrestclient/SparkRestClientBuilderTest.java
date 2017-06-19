package com.github.ywilkof.sparkrestclient;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by yonatan on 11.10.15.
 */
public class SparkRestClientBuilderTest {

    private SparkRestClient.SparkRestClientBuilder builder;

    @Before
    public void setUp() {
        builder = SparkRestClient.builder();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBuild_WhenHostNotSet_ThenThrowException() throws Exception {
        builder.masterPort(6066);
        builder.sparkVersion("2.1");
        builder.build();
    }

    @Test
    public void testBuild_WhenPortNotSet_ThenDontThrowException() throws Exception {
        builder.masterPort(null);
        builder.masterHost("somehost");
        builder.sparkVersion("2.1");
        builder.build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBuild_WhenClientIsNull_ThenThrowException() throws Exception {
        builder.masterHost("localhost");
        builder.masterPort(6066);
        builder.httpClient(null);
        builder.build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBuild_WhenSparkVersionIsNull_ThenThrowException() throws Exception {
        builder.masterHost("localhost");
        builder.masterPort(6066);
        builder.build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBuild_WhenSparkVersionIsEmpty_ThenThrowException() throws Exception {
        builder.masterHost("localhost");
        builder.masterPort(6066);
        builder.sparkVersion("");
        builder.build();
    }

    @Test
    public void testGetMasterUrl_WhenNoMasterApiUrlRoot_ThenReturnUrl() throws Exception {
        builder.masterHost("localhost");
        builder.masterPort(6066);
        builder.sparkVersion("some version");
        final SparkRestClient client = builder.build();
        Assert.assertEquals("localhost:6066",client.getMasterUrl());
    }

    @Test
    public void testGetMasterUrl_WhenWithMasterApiUrlRoot_ThenReturnUrl() throws Exception {
        builder.masterHost("localhost");
        builder.masterPort(6066);
        builder.sparkVersion("some version");
        builder.masterApiRoot("someurl");
        final SparkRestClient client = builder.build();
        Assert.assertEquals("localhost:6066/someurl",client.getMasterUrl());
    }
}