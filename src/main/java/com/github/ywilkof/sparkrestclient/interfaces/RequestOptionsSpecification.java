package com.github.ywilkof.sparkrestclient.interfaces;

/**
 * Created by yonatan on 17.10.15.
 */
public interface RequestOptionsSpecification {
    JobSubmitRequestSpecification prepareJobSubmit();
    KillJobRequestSpecification killJob();
    JobStatusRequestSpecification checkJobStatus();
}
