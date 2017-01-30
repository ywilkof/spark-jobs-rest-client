package com.github.ywilkof.sparkrestclient;

import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.client.BasicResponseHandler;

import java.io.IOException;

public class HttpRequestUtil {

    static <T extends SparkResponse>  T executeHttpMethodAndGetResponse(HttpClient client, HttpRequestBase httpRequest, Class<T> responseClass) throws FailedSparkRequestException {
        T response;
        try {
            final String stringResponse = client.execute(httpRequest, new BasicResponseHandler());
            if (stringResponse != null) {
                response = MapperWrapper.MAPPER.readValue(stringResponse, responseClass);
            } else {
                throw new FailedSparkRequestException("Received empty string response");
            }
        } catch (IOException e) {
            throw new FailedSparkRequestException(e);
        } finally {
            httpRequest.releaseConnection();
        }

        if (response == null) {
            throw new FailedSparkRequestException("An issue occured with the cluster's response.");
        }

        return response;
    }
}
