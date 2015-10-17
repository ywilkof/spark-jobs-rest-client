package com.github.ywilkof.sparkrestclient.interfaces;

import com.github.ywilkof.sparkrestclient.FailedSparkRequestException;

import java.util.List;
import java.util.Set;

/**
 * Created by yonatan on 17.10.15.
 */
public interface JobSubmitRequestSpecification {

    JobSubmitRequestSpecification appResource(String appResource);

    JobSubmitRequestSpecification appArgs(List<String> appArgs);

    JobSubmitRequestSpecification mainClass(String mainClass);

    JobSubmitRequestSpecification appName(String appName);

    JobSubmitRequestSpecification usingJars(Set<String> jars);

    SparkPropertiesSpecification withProperties();

    String submit() throws FailedSparkRequestException;

}
