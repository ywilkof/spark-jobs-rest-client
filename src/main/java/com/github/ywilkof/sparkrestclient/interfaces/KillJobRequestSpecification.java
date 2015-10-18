package com.github.ywilkof.sparkrestclient.interfaces;

import com.github.ywilkof.sparkrestclient.FailedSparkRequestException;

/**
 * Created by yonatan on 17.10.15.
 */
public interface KillJobRequestSpecification {
    boolean withSubmissionId(String submissionId) throws FailedSparkRequestException;
}
