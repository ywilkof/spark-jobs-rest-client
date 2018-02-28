package com.github.ywilkof.sparkrestclient;

import com.github.ywilkof.sparkrestclient.interfaces.CanValidateSubmissionId;
import com.github.ywilkof.sparkrestclient.interfaces.JobStatusRequestSpecification;
import org.apache.http.client.methods.HttpGet;

public class JobStatusRequestSpecificationImpl implements JobStatusRequestSpecification, CanValidateSubmissionId {

    private SparkRestClient sparkRestClient;

    public JobStatusRequestSpecificationImpl(SparkRestClient sparkRestClient) {
        this.sparkRestClient = sparkRestClient;
    }

    /**
     * Gets the status of an existing Driver Application
     * @param submissionId Id of submitted job to request status for.
     * @return State of the application
     * @throws FailedSparkRequestException Request to Spark server failed,
     * or the Spark Server could not retrieve the status of the requested app.
     */
    @Override
    public DriverState withSubmissionId(String submissionId) throws FailedSparkRequestException  {
        JobStatusResponse response = withSubmissionIdFullResponse(submissionId);
        return response.getDriverState();
    }

    /**
     * Gets the status of an existing Driver Application.
     * @param submissionId Id of submitted job to request status for.
     * @return Full state of the application, including information on the location
     * of the driver.
     * @throws FailedSparkRequestException Request to Spark server failed,
     * or the Spark Server could not retrieve the status of the requested app.
     */
    @Override
    public JobStatusResponse withSubmissionIdFullResponse(String submissionId) throws FailedSparkRequestException {
        assertSubmissionId(submissionId);
        final String url = sparkRestClient.getHttpScheme() + "://" + sparkRestClient.getMasterUrl() + "/v1/submissions/status/" + submissionId;
        final JobStatusResponse response = HttpRequestUtil.executeHttpMethodAndGetResponse(sparkRestClient.getClient(), new HttpGet(url),JobStatusResponse.class);
        if (!response.getSuccess()) {
            throw new FailedSparkRequestException("submit was not successful.", response.getDriverState());
        }
        return response;
    }
}
