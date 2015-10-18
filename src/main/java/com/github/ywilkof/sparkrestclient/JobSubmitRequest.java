package com.github.ywilkof.sparkrestclient;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Getter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    static class SparkProperties {

        @JsonProperty(value = "spark.jars")
        private String jars;

        @JsonProperty(value = "spark.app.name")
        private String appName;

        @JsonProperty(value = "spark.master")
        private String master;

        private Map<String,String> otherProperties = new HashMap<>();

        void setOtherProperties(String key, String value) {
            this.otherProperties.put(key,value);
        }

        @JsonAnyGetter
        Map<String,String> getOtherProperties() {
            return this.otherProperties;
        }

    }

}
