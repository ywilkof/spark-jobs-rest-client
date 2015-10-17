package com.github.ywilkof.sparkrestclient;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.ywilkof.sparkrestclient.interfaces.JobSubmitRequestSpecification;
import com.github.ywilkof.sparkrestclient.interfaces.SparkPropertiesSpecification;
import lombok.AccessLevel;
import lombok.Getter;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.protocol.HTTP;

import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Getter(AccessLevel.PACKAGE)
public class JobSubmitRequestSpecificationImpl implements JobSubmitRequestSpecification {

    private static final String CONTENT_TYPE_HEADER = "content-type";
    private static final String MIME_TYPE_JSON = "application/json";
    private static final String CHARSET_UTF_8 = "charset=UTF-8";
    private static final String MIME_TYPE_JSON_UTF_8 = MIME_TYPE_JSON + ";" + CHARSET_UTF_8;

    private String appResource;

    private List<String> appArgs;

    private String mainClass;

    private String appName;

    private Set<String> jars;

    private SparkRestClient sparkRestClient;

    public JobSubmitRequestSpecificationImpl(SparkRestClient sparkRestClient) {
        this.sparkRestClient = sparkRestClient;
    }

    /**
     * @param appResource location of jar which contains application containing your <code>mainClass</code>.
     * @return
     */
    @Override
    public JobSubmitRequestSpecification appResource(String appResource) {
        return this;
    }

    /**
     * @param appArgs args needed by the main() method of your <code>mainClass</code>.
     * @return
     */
    @Override
    public JobSubmitRequestSpecification appArgs(List<String> appArgs) {
        return this;
    }

    /**
     * @param mainClass class containing the main() method which defines the Spark application driver and tasks.
     * @return
     */
    @Override
    public JobSubmitRequestSpecification mainClass(String mainClass) {
        this.mainClass = mainClass;
        return this;
    }

    /**
     * @param appName name of your Spark job.
     * @return
     */
    @Override
    public JobSubmitRequestSpecification appName(String appName) {
        this.appName = appName;
        return this;
    }

    /**
     * @param jars other jars needed by the application and not supplied within the app or classpath.
     * @return
     */
    @Override
    public JobSubmitRequestSpecification usingJars(Set<String> jars) {
        this.jars = jars;
        return this;
    }

    @Override
    public SparkPropertiesSpecification withProperties() {
        return new SparkPropertiesSpecificationImpl(this);

    }

    /**
     * Submit a new spark job to the Spark Standalone cluster.
     * @return SubmissionId of task submitted to the Spark cluster, if submission was successful.
     * Please note that a successful submission does not guarantee successful deployment of app.
     * @throws FailedSparkRequestException iff submission failed..
     */
    public String submit() throws FailedSparkRequestException {
        if (mainClass == null || appResource  == null) {
            throw new IllegalArgumentException("mainClass and appResource values must not be null");
        }
        final JobSubmitRequest jobSubmitRequest = JobSubmitRequest.builder()
                .action(Action.CreateSubmissionRequest)
                .appArgs((appArgs == null) ? Collections.emptyList() : appArgs)
                .appResource(appResource)
                .clientSparkVersion(sparkRestClient.getSparkVersion().toString())
                .mainClass(mainClass)
                .environmentVariables(sparkRestClient.getEnvironmentVariables())
                .sparkProperties(
                        JobSubmitRequest.SparkProperties.builder()
                                .jars(jars(appResource, jars))
                                .appName(appName)
                                .eventLogEnabled(sparkRestClient.getEventLogEnabled())
                                .driverSupervise(sparkRestClient.getSupervise())
                                .master(sparkRestClient.getClusterMode() + "://" + sparkRestClient.getMasterUrl())
                                .build()
                )
                .build();

        final String url = "http://" + sparkRestClient.getMasterUrl() + "/v1/submissions/create";

        final HttpPost post = new HttpPost(url);
        post.setHeader(HTTP.CONTENT_TYPE, MIME_TYPE_JSON_UTF_8);

        try {
            final String message = MapperWrapper.MAPPER.writeValueAsString(jobSubmitRequest);
            post.setEntity(new StringEntity(message));
        } catch (UnsupportedEncodingException | JsonProcessingException e) {
            throw new FailedSparkRequestException(e);
        }

        final SparkResponse response = HttpRequestUtil.executeHttpMethodAndGetResponse(sparkRestClient.getClient(), post, SparkResponse.class);

        return response.getSubmissionId();
    }

    String jars(final String appResource, final Set<String> jars) {
        final Set<String> output = Stream.of(appResource).collect(Collectors.toSet());
        Optional.ofNullable(jars).ifPresent(j -> output.addAll(j));
        return String.join(",", new TreeSet<CharSequence>(output));
    }
}
