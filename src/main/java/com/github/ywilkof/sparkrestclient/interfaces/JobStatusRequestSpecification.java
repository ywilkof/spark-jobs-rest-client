package com.github.ywilkof.sparkrestclient.interfaces;

import com.github.ywilkof.sparkrestclient.DriverState;
import com.github.ywilkof.sparkrestclient.FailedSparkRequestException;
import com.github.ywilkof.sparkrestclient.JobStatusResponse;

/**
 * Created by yonatan on 17.10.15.
 */
public interface JobStatusRequestSpecification {
    DriverState withSubmissionId(String submissionId) throws FailedSparkRequestException;
    JobStatusResponse withSubmissionIdFullResponse(String submissionId) throws FailedSparkRequestException;
}
