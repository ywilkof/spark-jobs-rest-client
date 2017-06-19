package com.github.ywilkof.sparkrestclient;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

public class SparkRestClientTest {

    @Test
    public void getMasterUrl_WhenMasterApiRootAndPortGiven_ThenProduceCorrectUrl() throws Exception {

        final SparkRestClient subject = new SparkRestClient();
        subject.setMasterApiRoot("root");
        subject.setMasterPort(6066);
        subject.setMasterHost("host");
        Assert.assertEquals("host:6066/root", subject.getMasterUrl());
    }

    @Test
    public void getMasterUrl_WhenMasterApiRootNotGivenAndPortGiven_ThenProduceCorrectUrl() throws Exception {

        final SparkRestClient subject = new SparkRestClient();
        subject.setMasterPort(6066);
        subject.setMasterHost("host");
        Assert.assertEquals("host:6066", subject.getMasterUrl());

    }

    @Test
    public void getMasterUrl_WhenMasterApiRootGivenAndPortNNotGiven_ThenProduceCorrectUrl() throws Exception {

        final SparkRestClient subject = new SparkRestClient();
        subject.setMasterApiRoot("root");
        subject.setMasterHost("host");
        Assert.assertEquals("host/root", subject.getMasterUrl());

    }

    @Test
    public void getMasterUrl_WhenMasterApiRootNotGivenAndPortNNotGiven_ThenProduceCorrectUrl() throws Exception {

        final SparkRestClient subject = new SparkRestClient();
        subject.setMasterHost("host");
        Assert.assertEquals("host", subject.getMasterUrl());

    }

    @Test(expected = IllegalStateException.class)
    public void getMasterUrl_WhenMasterHost_ThenThrowException() throws Exception {

        final SparkRestClient subject = new SparkRestClient();
        subject.getMasterUrl();

    }

}