package com.gamegenetics.sparkrestclient;

import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.*;

/**
 * Created by yonatan on 08.10.15.
 */
public class SparkRestClientTest {

    private SparkRestClient sparkRestClient;

    @Before
    public void setUp() {
        this.sparkRestClient = new SparkRestClient();
        sparkRestClient.setSparkVersion(SparkVersion.V1_5_0);
        sparkRestClient.setMasterUrl("localhost:6066");
        sparkRestClient.setSupervise(false);
        sparkRestClient.setEventLogDisabled(Boolean.TRUE);
        sparkRestClient.setEnvironmentVariables(Collections.emptyMap());
    }

    @Test
    public void testSubmitJob() throws Exception {
        this.sparkRestClient.submitJob("bla","com.gamegenetics.sparkjobs", "file:/home/yonatan/gamegenetics/sparkjobs-scala/target/scala-2.10/sparkjobs-scala-assembly-1.0.jar",Collections.emptyList(), Collections.emptySet());
    }

    @Test
    public void testKillJob() throws Exception {

    }

    @Test
    public void testJobStatus() throws Exception {

    }
}