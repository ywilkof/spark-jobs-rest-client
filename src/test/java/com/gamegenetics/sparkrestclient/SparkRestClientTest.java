package com.gamegenetics.sparkrestclient;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.*;
import static org.hamcrest.Matchers.*;


/**
 * Created by yonatan on 08.10.15.
 */
public class SparkRestClientTest {

    private SparkRestClient sparkRestClient;

    @Rule
    public ExpectedException exception = ExpectedException.none();

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
        submitJob();
    }

    @Test
    public void testJars_WhenMultipleJarsSupplied_ThenReturnCommaSeparatedStringOfJars() {
        final String output = sparkRestClient.jars("a", Stream.of("b","c").collect(Collectors.toSet()));
        Assert.assertThat(output, equalTo("a,b,c"));
    }

    @Test
    public void testJars_WhenNullJarsSupplied_ThenReturnOnlyAppResourceJar() {
        final String output = sparkRestClient.jars("a", null);
        Assert.assertThat(output, equalTo("a"));
    }

    @Test
    public void testKillJob_WhenSubmissionIdIsRecognizedByMaster_ThenRequestIsSuccessful() throws Exception {
        final String submissionId = submitJob();
        sparkRestClient.killJob(submissionId);
    }

    @Test
    public void testKillJob_WhenSubmissionIdNotRecognizedByMaster_ThenThrowException() throws Exception {
        exception.expect(FailedSparkRequestException.class);
        exception.expectMessage(containsString("Spark master failed executing the kill"));
        sparkRestClient.killJob(UUID.randomUUID().toString());
    }

    @Test
    public void testJobStatus() throws Exception {
        final String submissionId = submitJob();
        sparkRestClient.jobStatus(submissionId);
    }

    private String submitJob() throws FailedSparkRequestException {
        return sparkRestClient.submitJob("bla", "com.gamegenetics.sparkjobs", "file:/home/yonatan/gamegenetics/sparkjobs-scala/target/scala-2.10/sparkjobs-scala-assembly-1.0.jar", Collections.emptyList(), Collections.emptySet());
    }
}