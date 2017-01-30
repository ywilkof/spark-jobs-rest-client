package com.github.ywilkof.sparkrestclient;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;

/**
 * Created by Yuval.Itzchakov on 30/01/2017.
 */
@Getter
@Setter(AccessLevel.PACKAGE)
public class JobStatusResponse extends SparkResponse {

    /**
     * The address of worker which was assigned the driver process.
     * Format: "[worker-ip]:[worker-port]"
     */
    private String workerHostPort;
    /**
     * The worker ID as assigned by Spark.
     */
    private String workerId;

    /**
     * Reports the status of the driver.
     */
    private DriverState driverState;
}
