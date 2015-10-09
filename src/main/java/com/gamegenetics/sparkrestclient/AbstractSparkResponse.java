package com.gamegenetics.sparkrestclient;

import lombok.Getter;

/**
 * Created by yonatan on 08.10.15.
 */
@Getter
abstract class AbstractSparkResponse {

    public AbstractSparkResponse() {}

    protected String message;

    protected SparkVersion serverSparkVersion;

    protected String submissionId;

    protected Boolean success;
}
