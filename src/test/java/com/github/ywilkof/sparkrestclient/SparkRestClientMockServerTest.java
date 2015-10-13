package com.github.ywilkof.sparkrestclient;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockserver.client.server.MockServerClient;
import org.mockserver.junit.MockServerRule;
import org.mockserver.model.Header;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.*;
import static org.mockserver.matchers.Times.exactly;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;
import static org.mockserver.model.JsonBody.json;


/**
 * Created by yonatan on 08.10.15.
 */
public class SparkRestClientMockServerTest {

    @Rule
    public MockServerRule mockServerRule = new MockServerRule(this);

    private MockServerClient mockServerClient;

    private SparkRestClient sparkRestClient;

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Before
    public void setUp() {
        this.sparkRestClient = SparkRestClient.builder()
                .masterHost("localhost")
                .masterPort(mockServerRule.getHttpPort())
        .build();
    }

    @Test
    public void testSubmitJob_WhenArgsAndJarsSupplied() throws Exception {
        final List<String> appArgs = Stream.of("A","B","C").collect(Collectors.toList());
        final Set<String> jars = Stream.of("/path/to/additional/jar/A.jar"
                ,"/path/to/additional/jar/B.jar")
                .collect(Collectors.toSet());

        mockServerJobSumbit(appArgs,jars);
        sparkRestClient.submitJob("SparkPiJob", "org.apache.spark.examples.SparkPi", "file:/path/to/jar",appArgs,jars);
    }

    @Test
    public void testSubmitJob_WhenArgsAndJarsNotSupplied() throws FailedSparkRequestException {
        mockServerJobSumbit(Collections.emptyList(),Collections.emptySet());
        sparkRestClient.submitJob("SparkPiJob", "org.apache.spark.examples.SparkPi", "file:/path/to/jar");
    }

    private void mockServerJobSumbit(final List<String> appArgs, final Set<String> jars) {
        final Set<String> allJars = new TreeSet<>(jars);
        allJars.add("file:/path/to/jar");

        final String requestBody = "{\n" +
                "  \"action\": \"CreateSubmissionRequest\",\n" +
                "  \"appResource\": \"file:/path/to/jar\",\n" +
                "  \"appArgs\": [\n" + String.join(",", appArgs) + "],\n" +
                "  \"clientSparkVersion\": \"1.5.0\",\n" +
                "  \"mainClass\": \"org.apache.spark.examples.SparkPi\",\n" +
                "  \"environmentVariables\": {\n" +
                "    \n" +
                "  },\n" +
                "  \"sparkProperties\": {\n" +
                "    \"spark.jars\": \""+  String.join(",",allJars) + "\",\n" +
                "    \"spark.app.name\": \"SparkPiJob\",\n" +
                "    \"spark.master\": \"spark://localhost:"+ mockServerRule.getHttpPort() + "\",\n" +
                "    \"spark.eventLog.enabled\": true,\n" +
                "    \"spark.driver.supervise\": false\n" +
                "  }\n" +
                "}";
        final String responseBody = "{\n" +
                "  \"action\" : \"CreateSubmissionResponse\",\n" +
                "  \"message\" : \"Driver successfully submitted as driver-20151008145126-0000\",\n" +
                "  \"serverSparkVersion\" : \"1.5.0\",\n" +
                "  \"submissionId\" : \"driver-20151008145126-0000\",\n" +
                "  \"success\" : true\n" +
                "}";
        mockServerClient
                .when(
                        request()
                                .withMethod("POST")
                                .withPath("/v1/submissions/create")
                                .withHeader(new Header("Content-Type","application/json;charset=UTF-8"))
                                .withBody(json((requestBody))),
                        exactly(1)
                )
                .respond(
                        response()
                                .withStatusCode(200)
                                .withBody(responseBody)
                );
    }


    @Test
    public void testKillJob_WhenSubmissionIdIsRecognizedByMaster_ThenRequestIsSuccessful() throws Exception {
        final String submissionId = UUID.randomUUID().toString();
        final String responseBody = "{\n" +
                "  \"action\" : \"KillSubmissionResponse\",\n" +
                "  \"message\" : \"Kill request for driver-" + submissionId + " submitted\",\n" +
                "  \"serverSparkVersion\" : \"1.5.0\",\n" +
                "  \"submissionId\" : \"driver-20151008145126-0000\",\n" +
                "  \"success\" : true\n" +
                "}";

        mockServerClient
                .when(
                        request()
                                .withMethod("POST")
                                .withPath("/v1/submissions/kill/" + submissionId),
                        exactly(1)
                )
                .respond(
                        response()
                                .withStatusCode(200)
                                .withBody(responseBody)
                );

        sparkRestClient.killJob(submissionId);
    }

    @Test
    public void testKillJob_WhenSubmissionIdNotRecognizedByMaster_ThenThrowException() throws Exception {
        final String submissionId = UUID.randomUUID().toString();
        final String responseBody = "{\n" +
                "  \"action\" : \"KillSubmissionResponse\",\n" +
                "  \"message\" : \"Kill request for driver-" + submissionId + " submitted\",\n" +
                "  \"serverSparkVersion\" : \"1.5.0\",\n" +
                "  \"submissionId\" : \"driver-" + submissionId + "\",\n" +
                "  \"success\" : false\n" +
                "}";

        mockServerClient
                .when(
                        request()
                                .withMethod("POST")
                                .withPath("/v1/submissions/kill/" + submissionId),
                        exactly(1)
                )
                .respond(
                        response()
                                .withStatusCode(200)
                                .withBody(responseBody)
                );
        exception.expect(FailedSparkRequestException.class);
        exception.expectMessage(containsString("Spark master failed executing the request."));
        sparkRestClient.killJob(submissionId);
    }

    @Test
    public void testJobStatus() throws Exception {
        final String submissionId = UUID.randomUUID().toString();
        final String responseBody = "{\n" +
                "  \"action\" : \"SubmissionStatusResponse\",\n" +
                "  \"driverState\" : \"FINISHED\",\n" +
                "  \"serverSparkVersion\" : \"1.5.0\",\n" +
                "  \"submissionId\" : \"driver-" + submissionId + "\",\n" +
                "  \"success\" : true,\n" +
                "  \"workerHostPort\" : \"192.168.3.153:46894\",\n" +
                "  \"workerId\" : \"worker-20151007093409-192.168.3.153-46894\"\n" +
                "}";
        mockServerClient
                .when(
                        request()
                                .withMethod("GET")
                                .withPath("/v1/submissions/status/" + submissionId),
                        exactly(1)
                )
                .respond(
                        response()
                                .withStatusCode(200)
                                .withBody(responseBody)
                );
        Assert.assertThat(sparkRestClient.jobStatus(submissionId), equalTo(DriverState.FINISHED));
    }

    @Test
    public void testJobStatus_WhenDriverStateNotEnumValue_thenThrowException() throws Exception {
        final String submissionId = UUID.randomUUID().toString();
        final String responseBody = "{\n" +
                "  \"action\" : \"SubmissionStatusResponse\",\n" +
                "  \"driverState\" : \"HIBERNATING\",\n" +
                "  \"serverSparkVersion\" : \"1.5.0\",\n" +
                "  \"submissionId\" : \"driver-" + submissionId + "\",\n" +
                "  \"success\" : true,\n" +
                "  \"workerHostPort\" : \"192.168.3.153:46894\",\n" +
                "  \"workerId\" : \"worker-20151007093409-192.168.3.153-46894\"\n" +
                "}";
        mockServerClient
                .when(
                        request()
                                .withMethod("GET")
                                .withPath("/v1/submissions/status/" + submissionId),
                        exactly(1)
                )
                .respond(
                        response()
                                .withStatusCode(200)
                                .withBody(responseBody)
                );
        exception.expect(FailedSparkRequestException.class);
        exception.expectMessage(containsString("Spark server responded with different values than expected."));
        sparkRestClient.jobStatus(submissionId);
    }

}