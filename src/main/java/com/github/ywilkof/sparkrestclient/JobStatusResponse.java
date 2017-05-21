package com.github.ywilkof.sparkrestclient;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;

import java.util.Optional;

/**
 * Created by Yuval.Itzchakov on 30/01/2017.
 */
@Setter(AccessLevel.PACKAGE)
public class JobStatusResponse extends SparkResponse {

    /**
     * The address of worker which was assigned the driver process.
     * Format: "[worker-ip]:[worker-port]"
     */
    private String workerHostPort;

    public Optional<String> getWorkerHostPort() {
        return Optional.ofNullable(workerHostPort);
    }

    /**
     * The worker ID as assigned by Spark.
     */
    private String workerId;

    public Optional<String> getWorkerId() {
        return Optional.ofNullable(workerId);
    }

    /**
     * Reports the status of the driver.
     */
    @Getter private DriverState driverState;
}
