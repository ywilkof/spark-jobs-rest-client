package com.github.ywilkof.sparkrestclient;

/**
 * Created by yonatan on 17.10.15.
 */
public interface RequestSpecification {
    JobSubmitRequestSpecification prepareJobSubmit();
    KillJobRequestSpecification killJob();
    JobStatusRequestSpecification checkJobStatus();
}
