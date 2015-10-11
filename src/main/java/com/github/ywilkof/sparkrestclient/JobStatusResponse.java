package com.github.ywilkof.sparkrestclient;

import lombok.Getter;

/**
 * Created by yonatan on 08.10.15.
 */
@Getter
class JobStatusResponse extends SparkResponse {

    JobStatusResponse() {}

    private DriverState driverState;

    private String workerHostPort;

    private String workerId;

}
