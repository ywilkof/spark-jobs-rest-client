package com.github.ywilkof.sparkrestclient;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.BasicHttpClientConnectionManager;
import org.apache.http.protocol.HTTP;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by yonatan on 08.10.15.
 */
@Setter(AccessLevel.PACKAGE)
@Getter(AccessLevel.PUBLIC)
public class SparkRestClient {

    SparkRestClient() {}

    private SparkVersion sparkVersion;

    private Integer masterPort;

    private String masterHost;

    private Boolean eventLogEnabled;

    private Boolean supervise;

    private ClusterMode clusterMode;

    private Map<String,String> environmentVariables;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String CONTENT_TYPE_HEADER = "content-type";
    private static final String MIME_TYPE_JSON = "application/json";
    private static final String DEPLOY_MODE_CLUSTER = "cluster";
    private static final String CHARSET_UTF_8 = "charset=UTF-8";
    private static final String MIME_TYPE_JSON_UTF_8 = MIME_TYPE_JSON + ";" + CHARSET_UTF_8;

    private HttpClient client;

    /**
     * Submit a new spark job with no command line args and no additional jars to the Spark Standalone cluster.
     * @param appName name of your Spark job.
     * @param mainClass class containing the main() method which defines the Spark application driver and tasks.
     * @param appResource location of jar which contains application containing your <code>mainClass</code>.
     * @return SubmissionId of task submitted to the Spark cluster, if submission was successful.
     * Please note that a successful submission does not guarantee successful deployment of app.
     * @throws FailedSparkRequestException
     */
    public String submitJob(final String appName,
                            final String mainClass,
                            final String appResource) throws FailedSparkRequestException {
        return submitJob(appName,mainClass,appResource,null,null);
    }

    /**
     * Submit a new spark job to the Spark Standalone cluster.
     * @param appName name of your Spark job.
     * @param mainClass class containing the main() method which defines the Spark application driver and tasks.
     * @param appResource location of jar which contains application containing your <code>mainClass</code>.
     * @param appArgs args needed by the main() method of your <code>mainClass</code>.
     * @param jars other jars needed by the application and not supplied within the app or classpath.
     * @return SubmissionId of task submitted to the Spark cluster, if submission was successful.
     * Please note that a successful submission does not guarantee successful deployment of app.
     * @throws FailedSparkRequestException iff submission failed..
     */
    public String submitJob(final String appName,
                            final String mainClass,
                            final String appResource,
                            final List<String> appArgs,
                            final Set<String> jars) throws FailedSparkRequestException {
        if (mainClass == null || appResource  == null) {
            throw new IllegalArgumentException("mainClass and appResource values must not be null");
        }
        final JobSubmitRequest jobSubmitRequest = JobSubmitRequest.builder()
                .action(Action.CreateSubmissionRequest)
                .appArgs((appArgs == null) ? Collections.emptyList() : appArgs)
                .appResource(appResource)
                .clientSparkVersion(sparkVersion.toString())
                .mainClass(mainClass)
                .environmentVariables(environmentVariables)
                .sparkProperties(
                        JobSubmitRequest.SparkProperties.builder()
                                .jars(jars(appResource, jars))
                                .appName(appName)
                                .eventLogEnabled(eventLogEnabled)
                                .driverSupervise(supervise)
                                .master(clusterMode + "://" + getMasterUrl())
                                .build()
                )
                .build();

        final String url = "http://" + getMasterUrl() + "/v1/submissions/create";

        final HttpPost post = new HttpPost(url);
        post.setHeader(HTTP.CONTENT_TYPE, MIME_TYPE_JSON_UTF_8);

        try {
            final String message = MAPPER.writeValueAsString(jobSubmitRequest);
            post.setEntity(new StringEntity(message));
        } catch (UnsupportedEncodingException | JsonProcessingException e) {
            throw new FailedSparkRequestException(e);
        }

        final SparkResponse response = executeHttpMethodAndGetResponse(post, SparkResponse.class);

        return response.getSubmissionId();
    }

    String jars(final String appResource, final Set<String> jars) {
        final Set<String> output = Stream.of(appResource).collect(Collectors.toSet());
        Optional.ofNullable(jars).ifPresent(j -> output.addAll(j));
        return String.join(",", new TreeSet<CharSequence>(output));
    }

    /**
     * Submits a kill request for an existing Driver Application.
     * @param submissionId Id of submitted job to submit a kill request for.
     * @throws FailedSparkRequestException Request to Spark server failed,
     * or the Spark Server could not kill the requested app.
     */
    public void killJob(final String submissionId) throws FailedSparkRequestException {
        assertSubmissionId(submissionId);
        final String url = "http://" + getMasterUrl() + "/v1/submissions/kill/" + submissionId;
        executeHttpMethodAndGetResponse(new HttpPost(url), SparkResponse.class);
    }

    /**
     * Gets the status of an existing Driver Application
     * @param submissionId Id of submitted job to request status for.
     * @return State of the application
     * @throws FailedSparkRequestException Request to Spark server failed,
     * or the Spark Server could not retrieve the status of the requested app.
     */
    public DriverState jobStatus(final String submissionId) throws FailedSparkRequestException {
        assertSubmissionId(submissionId);
        final String url = "http://" + getMasterUrl() + "/v1/submissions/status/" + submissionId;
        final JobStatusResponse response = executeHttpMethodAndGetResponse(new HttpGet(url),JobStatusResponse.class);
        return response.getDriverState();
    }

    private<T extends SparkResponse>  T executeHttpMethodAndGetResponse(HttpRequestBase httpRequest, Class<T> responseClass) throws FailedSparkRequestException {
        T response;
        try {
            final String stringResponse = client.execute(httpRequest, new BasicResponseHandler());
            if (stringResponse != null) {
                response = (T) MAPPER.readValue(stringResponse, responseClass);
            } else {
                throw new FailedSparkRequestException("Received empty string response");
            }
        } catch (InvalidFormatException e) {
            throw new FailedSparkRequestException("Spark server responded with different values than expected.");
        } catch (IOException e) {
            throw new FailedSparkRequestException(e);
        } finally {
            httpRequest.releaseConnection();
        }

        if ( response == null || !response.getSuccess()) {
            throw new FailedSparkRequestException("Spark master failed executing the request.");
        }

        return response;
    }

    private void assertSubmissionId(final String submissionId) {
        if (submissionId == null
                || submissionId.isEmpty()
                || submissionId.trim().equals("")) {
            throw new IllegalArgumentException("SubmissionId must be a non blank string");
        }
    }

    private String getMasterUrl() {
        return masterHost + ":" + masterPort;
    }

    public static SparkRestClientBuilder builder() {
        return new SparkRestClientBuilder();
    }

    public static class SparkRestClientBuilder {
        private SparkVersion sparkVersion = SparkVersion.V1_5_0;
        private Integer masterPort = 6066;
        private String masterHost;
        private Boolean eventLogEnabled = Boolean.FALSE;
        private Boolean supervise = Boolean.FALSE;
        private ClusterMode clusterMode = ClusterMode.spark;

        private Map<String,String> environmentVariables = Collections.emptyMap();

        private HttpClient client = HttpClientBuilder.create()
                .setConnectionManager(new BasicHttpClientConnectionManager())
                .build();


        private SparkRestClientBuilder() {
        }

        public SparkRestClientBuilder sparkVersion(SparkVersion sparkVersion) {
            this.sparkVersion = sparkVersion;
            return this;
        }

        public SparkRestClientBuilder masterPort(Integer masterPort) {
            this.masterPort = masterPort;
            return this;
        }

        public SparkRestClientBuilder masterHost(String masterHost) {
            this.masterHost = masterHost;
            return this;
        }

        public SparkRestClientBuilder eventLogEnabled(Boolean eventLogEnabled) {
            this.eventLogEnabled = eventLogEnabled;
            return this;
        }

        public SparkRestClientBuilder supervise(Boolean supervise) {
            this.supervise = supervise;
            return this;
        }

        public SparkRestClientBuilder clusterMode(ClusterMode clusterMode) {
            this.clusterMode = clusterMode;
            return this;
        }

        public SparkRestClientBuilder environmentVariables(Map<String, String> environmentVariables) {
            this.environmentVariables = environmentVariables;
            return this;
        }

        public SparkRestClientBuilder httpClient(HttpClient httpClient) {
            this.client = httpClient;
            return this;
        }

        public SparkRestClient build() {
            if (masterHost == null ||
                    masterPort == null) {
                throw new IllegalArgumentException("master host and port must be set.");
            }
            if (client == null) {
                throw new IllegalArgumentException("http client cannot be null.");
            }
            SparkRestClient sparkRestClient = new SparkRestClient();
            sparkRestClient.setSparkVersion(sparkVersion);
            sparkRestClient.setMasterPort(masterPort);
            sparkRestClient.setMasterHost(masterHost);
            sparkRestClient.setEventLogEnabled(eventLogEnabled);
            sparkRestClient.setSupervise(supervise);
            sparkRestClient.setEnvironmentVariables(environmentVariables);
            sparkRestClient.setClient(client);
            sparkRestClient.setClusterMode(clusterMode);
            return sparkRestClient;
        }
    }
}
