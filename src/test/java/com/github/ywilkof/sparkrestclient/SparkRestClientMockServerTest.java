package com.github.ywilkof.sparkrestclient;

import org.hamcrest.Matchers;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.mockserver.client.server.MockServerClient;
import org.mockserver.junit.MockServerRule;
import org.mockserver.model.Header;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
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

        mockServerJobSubmit(getExpectedRequest(appArgs,jars));
        Assert.assertThat(sparkRestClient.prepareJobSubmit()
                .appArgs(appArgs)
                .appName("SparkPiJob")
                .appResource("file:/path/to/jar")
                .mainClass("org.apache.spark.examples.SparkPi")
                .usingJars(jars)
                .submit(), equalTo("driver-20151008145126-0000"));
    }

    @Test
    public void testSubmitJob_WhenArgsAndJarsNotSupplied() throws FailedSparkRequestException {
        mockServerJobSubmit(getExpectedRequest(Collections.emptyList(), Collections.emptySet()));
        Assert.assertThat(sparkRestClient.prepareJobSubmit()
                .appName("SparkPiJob")
                .appResource("file:/path/to/jar")
                .mainClass("org.apache.spark.examples.SparkPi")
                .submit(), equalTo("driver-20151008145126-0000"));
    }

    @Test
    public void testSubmitJob_WhenPropertiesSupplied() throws FailedSparkRequestException {
        final String requestBody = "{\n" +
                "  \"action\": \"CreateSubmissionRequest\",\n" +
                "  \"appResource\": \"file:\\/path\\/to\\/jar\",\n" +
                "  \"appArgs\": [\n" +
                "  ],\n" +
                "  \"clientSparkVersion\": \"1.5.0\",\n" +
                "  \"mainClass\": \"org.apache.spark.examples.SparkPi\",\n" +
                "  \"environmentVariables\": {\n" +
                "  },\n" +
                "  \"sparkProperties\": {\n" +
                "    \"spark.jars\": \"file:/path/to/jar\",\n" +
                "    \"spark.app.name\": \"SparkPiJob\",\n" +
                "    \"spark.master\": \"spark://localhost:" + mockServerRule.getHttpPort() + "\",\n" +
                "    \"spark.executor.memory\": \"4g\",\n" +
                "    \"spark.driver.memory\": \"2g\",\n" +
                "    \"spark.driver.cores\": \"2\",\n" +
                "    \"spark.executor.cores\": \"3\"\n" +
                "  }\n" +
                "}";
        mockServerJobSubmit(requestBody);
        Assert.assertThat(sparkRestClient.prepareJobSubmit()
                .appName("SparkPiJob")
                .appResource("file:/path/to/jar")
                .mainClass("org.apache.spark.examples.SparkPi")
                .withProperties()
                .put("spark.driver.memory","2g")
                .put("spark.driver.cores","2")
                .put("spark.executor.cores","3")
                .put("spark.executor.memory","4g")
                .submit(), Matchers.equalTo("driver-20151008145126-0000"));
    }

    private void mockServerJobSubmit(String expectedRequestBody) {
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
                                .withHeader(new Header("Content-Type", "application/json;charset=UTF-8"))
                                .withBody(json((expectedRequestBody))),
                        exactly(1)
                )
                .respond(
                        response()
                                .withStatusCode(200)
                                .withBody(responseBody)
                );
    }

    private String getExpectedRequest(List<String> appArgs, Set<String> jars) {
        final Set<String> allJars = new TreeSet<>(jars);
        allJars.add("file:/path/to/jar");
        return "{\n" +
                    "  \"action\": \"CreateSubmissionRequest\",\n" +
                    "  \"appResource\": \"file:/path/to/jar\",\n" +
                    "  \"appArgs\": [" + String.join(",", appArgs) + "],\n" +
                    "  \"clientSparkVersion\": \"1.5.0\",\n" +
                    "  \"mainClass\": \"org.apache.spark.examples.SparkPi\",\n" +
                    "  \"environmentVariables\": {\n" +
                    "    \n" +
                    "  },\n" +
                    "  \"sparkProperties\": {\n" +
                    "    \"spark.jars\": \""+  String.join(",",allJars) + "\",\n" +
                    "    \"spark.app.name\": \"SparkPiJob\",\n" +
                    "    \"spark.master\": \"spark://localhost:"+ mockServerRule.getHttpPort() + "\",\n" +
                    "  }\n" +
                    "}";
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

        Assert.assertThat(sparkRestClient.killJob().withSubmissionId(submissionId), is(true));
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
        sparkRestClient.killJob().withSubmissionId(submissionId);
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
        Assert.assertThat(sparkRestClient.checkJobStatus().withSubmissionId(submissionId), equalTo(DriverState.FINISHED));
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
        sparkRestClient.checkJobStatus().withSubmissionId(submissionId);
    }

}