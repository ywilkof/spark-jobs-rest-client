package com.github.ywilkof.sparkrestclient;

/**
 * Created by yonatan on 17.10.15.
 */
public interface SparkPropertiesSpecification {

    SparkPropertiesSpecification supervise(boolean supervise);

    SparkPropertiesSpecification eventLogEnabled(boolean eventLogEnabled);

    SparkPropertiesSpecification driverMemory(String driverMemory);

    SparkPropertiesSpecification driverCores(Integer driverCores);

    SparkPropertiesSpecification driverExtraJavaOptions(String driverExtraJavaOptions);

    SparkPropertiesSpecification driverExtraClassPath(String driverExtraClassPath);

    SparkPropertiesSpecification driverExtraLibraryPath(String driverExtraLibraryPath);

    String submit() throws FailedSparkRequestException;

}
