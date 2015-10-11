package com.ywilkof.sparkrestclient;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Getter;

/**
 * Created by yonatan on 08.10.15.
 */
@Getter
@JsonIgnoreProperties(ignoreUnknown = true)
class SparkResponse {

    SparkResponse() {}

    private Action action;

    protected String message;

    protected SparkVersion serverSparkVersion;

    protected String submissionId;

    protected Boolean success;
}
