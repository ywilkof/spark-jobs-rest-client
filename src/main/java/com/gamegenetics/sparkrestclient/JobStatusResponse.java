package com.gamegenetics.sparkrestclient;

import lombok.Getter;

/**
 * Created by yonatan on 08.10.15.
 */
@Getter
class JobStatusResponse extends AbstractSparkResponse{

    private Action action;

    private String driverState;

    private String workerHostPort;

    private String workerId;

}
