package com.github.ywilkof.sparkrestclient;

import com.github.ywilkof.sparkrestclient.interfaces.CanValidateSubmissionId;
import com.github.ywilkof.sparkrestclient.interfaces.KillJobRequestSpecification;
import org.apache.http.client.methods.HttpPost;

public class KillJobRequestSpecificationImpl implements KillJobRequestSpecification,CanValidateSubmissionId {

    private SparkRestClient sparkRestClient;

    public KillJobRequestSpecificationImpl(SparkRestClient sparkRestClient) {
        this.sparkRestClient = sparkRestClient;
    }

    /**
     * Submits a kill request for an existing Driver Application.
     * @param submissionId Id of submitted job to submit a kill request for.
     * @throws FailedSparkRequestException Request to Spark server failed,
     * or the Spark Server could not kill the requested app.
     */
    @Override
    public boolean withSubmissionId(String submissionId) throws FailedSparkRequestException {
        assertSubmissionId(submissionId);
        final String url = "http://" + sparkRestClient.getMasterUrl() + "/v1/submissions/kill/" + submissionId;
        return HttpRequestUtil
                .executeHttpMethodAndGetResponse(sparkRestClient.getClient(), new HttpPost(url), SparkResponse.class)
                .getSuccess();
    }

}
