package com.ywilkof.sparkrestclient;

import lombok.Builder;

import java.util.Collections;
import java.util.Map;

/**
 * Created by yonatan on 08.10.15.
 */
@Builder
class SparkRestClientFactory {

    private static final Integer SPARK_PORT = 6066;

    private SparkVersion sparkVersion = SparkVersion.V1_5_0;

    private Integer port = SPARK_PORT;

    private String masterHostname;

    private Boolean supervise = Boolean.FALSE;

    private Boolean eventLogDisabled = Boolean.TRUE;

    private Map<String,String> environmentVariables = Collections.emptyMap();

    public SparkRestClient getClient() {
        final  SparkRestClient sparkRestClient = new SparkRestClient();
        sparkRestClient.setSupervise(supervise);
        sparkRestClient.setMasterUrl(masterHostname + ":" + SPARK_PORT);
        return sparkRestClient;
    }




}
