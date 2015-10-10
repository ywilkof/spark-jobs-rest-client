package ywilkof.sparkrestclient;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.EnumSet;

/**
 * Created by yonatan on 08.10.15.
 */
enum SparkVersion {

    V1_5_0("1.5.0");

    private String version;

    SparkVersion(String version) {
        this.version = version;
    }

    @JsonValue
    public String getVersion() {
        return version;
    }

    @JsonCreator
    public static SparkVersion fromVersion(final String version) {
        for (SparkVersion sparkVersion : EnumSet.allOf(SparkVersion.class)) {
            if (sparkVersion.getVersion().equals(version)) {
                return sparkVersion;
            }
        };
        throw new IllegalArgumentException("Invalid Spark version: " + version);
    }

    @Override
    public String toString() {
        return this.version;
    }
}
