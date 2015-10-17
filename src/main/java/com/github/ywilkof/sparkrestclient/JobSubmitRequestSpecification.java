package com.github.ywilkof.sparkrestclient;

import java.util.List;

/**
 * Created by yonatan on 17.10.15.
 */
public interface JobSubmitRequestSpecification {

    JobSubmitRequestSpecification appResource(String appResource);

    JobSubmitRequestSpecification appArgs(List<String> appArgs);

    JobSubmitRequestSpecification mainClass(String mainClass);

    JobSubmitRequestSpecification appName(String appName);

    SparkPropertiesSpecification withProperties();

    String submit() throws FailedSparkRequestException;

}
