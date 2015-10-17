package com.github.ywilkof.sparkrestclient;

import com.github.ywilkof.sparkrestclient.interfaces.CanValidateSubmissionId;
import org.junit.Test;

public class CanValidateSubmissionIdTest {

    @Test(expected = IllegalArgumentException.class)
    public void testAssertSubmissionId_WhenStringNull() throws Exception {
        final CanValidateSubmissionId canValidateSubmissionId = new CanValidateSubmissionId() {};
        canValidateSubmissionId.assertSubmissionId(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAssertSubmissionId_WhenStringBlank() throws Exception {
        final CanValidateSubmissionId canValidateSubmissionId = new CanValidateSubmissionId() {};
        canValidateSubmissionId.assertSubmissionId(" ");
    }
}