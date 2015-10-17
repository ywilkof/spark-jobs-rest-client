package com.github.ywilkof.sparkrestclient;

import com.github.ywilkof.sparkrestclient.interfaces.JobSubmitRequestSpecification;
import com.github.ywilkof.sparkrestclient.interfaces.SparkPropertiesSpecification;

public class SparkPropertiesSpecificationImpl implements SparkPropertiesSpecification {

    private JobSubmitRequestSpecification submitRequestSpecification;

    public SparkPropertiesSpecificationImpl(JobSubmitRequestSpecification submitRequestSpecification) {
        this.submitRequestSpecification = submitRequestSpecification;
    }

    @Override
    public SparkPropertiesSpecification supervise(boolean supervise) {
        return this;
    }

    @Override
    public SparkPropertiesSpecification eventLogEnabled(boolean eventLogEnabled) {
        return this;
    }

    @Override
    public SparkPropertiesSpecification driverMemory(String driverMemory) {
        return this;
    }

    @Override
    public SparkPropertiesSpecification driverCores(Integer driverCores) {
        return this;
    }

    @Override
    public SparkPropertiesSpecification driverExtraJavaOptions(String driverExtraJavaOptions) {
        return this;
    }

    @Override
    public SparkPropertiesSpecification driverExtraClassPath(String driverExtraClassPath) {
        return this;
    }

    @Override
    public SparkPropertiesSpecification driverExtraLibraryPath(String driverExtraLibraryPath) {
        return this;
    }

    @Override
    public String submit() throws FailedSparkRequestException {
        return "";
    }
}
