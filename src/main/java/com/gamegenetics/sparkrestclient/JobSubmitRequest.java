package com.gamegenetics.sparkrestclient;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Getter;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by yonatan on 08.10.15.
 */
@Getter
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
class JobSubmitRequest {

    private Action action;

    private String appResource;

    private List<String> appArgs;

    private String clientSparkVersion;

    private String mainClass;

    private Map<String,String> environmentVariables;

    private SparkProperties sparkProperties;

    @Builder
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class SparkProperties {

        @JsonProperty(value = "spark.jars")
        private String jars;

        @JsonProperty(value = "spark.app.name")
        private String appName;

        @JsonProperty(value = "spark.master")
        private String master;

        @JsonProperty(value = "spark.eventLog.enabled")
        private Boolean eventLogEnabled;

        @JsonProperty(value = "spark.driver.supervise")
        private Boolean driverSupervise;

        @JsonProperty(value = "spark.driver.memory")
        private Integer driverMemory;

        @JsonProperty(value = "spark.driver.cores")
        private Integer driverCores;

        @JsonProperty(value = "spark.driver.extraJavaOptions")
        private String driverExtraJavaOptions;

        @JsonProperty(value = "spark.driver.extraClassPath")
        private String driverExtraClassPath;

        @JsonProperty(value = "spark.driver.extraLibraryPath")
        private String driverExtraLibraryPath;
    }

}
