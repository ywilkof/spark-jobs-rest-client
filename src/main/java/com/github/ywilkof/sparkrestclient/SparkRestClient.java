package com.github.ywilkof.sparkrestclient;

import com.github.ywilkof.sparkrestclient.interfaces.JobStatusRequestSpecification;
import com.github.ywilkof.sparkrestclient.interfaces.JobSubmitRequestSpecification;
import com.github.ywilkof.sparkrestclient.interfaces.KillJobRequestSpecification;
import com.github.ywilkof.sparkrestclient.interfaces.RequestOptionsSpecification;

import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.BasicHttpClientConnectionManager;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

import java.util.*;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;

/**
 * Created by yonatan on 08.10.15.
 */
@Setter(AccessLevel.PACKAGE)
@Getter(AccessLevel.PUBLIC)
public class SparkRestClient implements RequestOptionsSpecification {

    SparkRestClient() {}

    private String sparkVersion;

    private Integer masterPort;

    private String httpScheme;

    private String masterHost;

    private String masterApiRoot;

    private ClusterMode clusterMode;

    private Map<String,String> environmentVariables;

    private static final String DEPLOY_MODE_CLUSTER = "cluster";

    private HttpClient client;

    String getMasterUrl() {

        final String host = Optional
                .ofNullable(masterHost)
                .filter(s -> !s.isEmpty())
                .orElseThrow(() -> new IllegalStateException("master host must be set before getting master url"));

        final Optional<String> maybeMasterApiRoot = Optional
                .ofNullable(masterApiRoot)
                .filter(s -> !s.isEmpty());

        final Optional<Integer> maybePort = Optional
                .ofNullable(masterPort);

        if (maybeMasterApiRoot.isPresent() && maybePort.isPresent())  {
            return host + ":" + masterPort + "/" + masterApiRoot;
        }

        if (!maybeMasterApiRoot.isPresent() && maybePort.isPresent())  {
            return host + ":" + masterPort;
        }

        if (maybeMasterApiRoot.isPresent() && !maybePort.isPresent())  {
            return host + "/" + masterApiRoot;
        }

        return host;

    }

    public static SparkRestClientBuilder builder() {
        return new SparkRestClientBuilder();
    }

    @Override
    public JobSubmitRequestSpecification prepareJobSubmit() {
        return new JobSubmitRequestSpecificationImpl(this);
    }

    @Override
    public KillJobRequestSpecification killJob() {
        return new KillJobRequestSpecificationImpl(this);
    }

    @Override
    public JobStatusRequestSpecification checkJobStatus() {
        return new JobStatusRequestSpecificationImpl(this);
    }

    public static class SparkRestClientBuilder {
        private String sparkVersion;
        private Integer masterPort = 6066;
        private String masterHost;
        private String masterApiRoot;
        private String httpScheme = "http";
        private ClusterMode clusterMode = ClusterMode.spark;

        private Map<String,String> environmentVariables = Collections.emptyMap();

        private HttpClient client = HttpClientBuilder.create()
                .setConnectionManager(new BasicHttpClientConnectionManager())
                .build();

        private SparkRestClientBuilder() {
        }

        public SparkRestClientBuilder sparkVersion(String sparkVersion) {
            this.sparkVersion = sparkVersion;
            return this;
        }

        public SparkRestClientBuilder masterPort(Integer masterPort) {
            this.masterPort = masterPort;
            return this;
        }

        public SparkRestClientBuilder masterApiRoot(String masterApiRoot) {
            if (masterApiRoot.startsWith("/")) {
              masterApiRoot = masterApiRoot.substring(1);
            }
            this.masterApiRoot = masterApiRoot;
            return this;
        }

        public SparkRestClientBuilder httpScheme(String httpScheme) {
            this.httpScheme = (httpScheme != null) ? httpScheme.toLowerCase() : null;
            return this;
        }

        public SparkRestClientBuilder masterHost(String masterHost) {
            this.masterHost = masterHost;
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

        private final static Set<String> ALLOWED_HTTP_SCHEMES = new HashSet<>(Arrays.asList("http","https"));

        public SparkRestClientBuilder poolingHttpClient(int maxTotalConnections) {
            final PoolingHttpClientConnectionManager poolingHttpClientConnectionManager = new PoolingHttpClientConnectionManager();

            poolingHttpClientConnectionManager.setMaxTotal(maxTotalConnections);
            poolingHttpClientConnectionManager.setDefaultMaxPerRoute(maxTotalConnections); // we will have only one route - spark master
            this.client = HttpClientBuilder.create().setConnectionManager(poolingHttpClientConnectionManager).build();
            return this;
        }

        public SparkRestClient build() {
            if (masterHost == null) {
                throw new IllegalArgumentException("master host must be set.");
            }
            if (client == null) {
                throw new IllegalArgumentException("http client cannot be null.");
            }
            if (sparkVersion == null || sparkVersion.isEmpty()) {
                throw new IllegalArgumentException("spark version is not set.");
            }
            if (!ALLOWED_HTTP_SCHEMES.contains(httpScheme)) {
                throw new IllegalArgumentException("Supported http schemes are: [" + String.join(",", ALLOWED_HTTP_SCHEMES) + "]" );
            }
            SparkRestClient sparkRestClient = new SparkRestClient();
            sparkRestClient.setSparkVersion(sparkVersion);
            sparkRestClient.setMasterPort(masterPort);
            sparkRestClient.setMasterHost(masterHost);
            sparkRestClient.setHttpScheme(httpScheme);
            sparkRestClient.setMasterApiRoot(masterApiRoot);
            sparkRestClient.setEnvironmentVariables(environmentVariables);
            sparkRestClient.setClient(client);
            sparkRestClient.setClusterMode(clusterMode);
            return sparkRestClient;
        }
    }
}
