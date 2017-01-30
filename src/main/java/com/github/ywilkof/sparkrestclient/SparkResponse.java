package com.github.ywilkof.sparkrestclient;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;

/**
 * Created by yonatan on 08.10.15.
 */
@Getter
@Setter(AccessLevel.PACKAGE)
@JsonIgnoreProperties(ignoreUnknown = true)
class SparkResponse {
    SparkResponse() {}

    private Action action;

    /**
     * Message status.
     */
    private String message;

    /**
     * Spark version.
     */
    private String serverSparkVersion;

    /**
     * The submission ID as assigned by Spark.
     */
    private String submissionId;

    /**
     * Marks the success or failure of the submission.
     */
    private Boolean success;
}
