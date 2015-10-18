package com.github.ywilkof.sparkrestclient.interfaces;

/**
 * Created by yonatan on 10/17/15.
 */
public interface CanValidateSubmissionId {
    default void assertSubmissionId(final String submissionId) {
        if (submissionId == null
                || submissionId.isEmpty()
                || submissionId.trim().equals("")) {
            throw new IllegalArgumentException("SubmissionId must be a non blank string");
        }
    }
}
