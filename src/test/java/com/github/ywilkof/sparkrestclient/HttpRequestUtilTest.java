package com.github.ywilkof.sparkrestclient;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Getter;
import org.apache.http.HttpRequest;
import org.apache.http.client.HttpClient;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpRequestBase;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.*;

/**
 * Created by yonatan on 11.11.15.
 */
@RunWith(MockitoJUnitRunner.class)
public class HttpRequestUtilTest {

    @Mock
    private HttpClient httpClient;

    @Mock
    private HttpRequestBase httpRequestBase;

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Getter
    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class TestSparkResponse extends SparkResponse {
        TestSparkResponse(){}
    }

    @Test
    public void testExecuteHttpMethodAndGetResponse__WhenRecievingNullStringResponse_ThenThrowExcpetion() throws Exception {
        Mockito.doReturn(null).when(httpClient).execute(Matchers.any(HttpRequestBase.class),Matchers.any(ResponseHandler.class));
        exception.expect(FailedSparkRequestException.class);
        exception.expectMessage("Received empty string response");
        HttpRequestUtil.executeHttpMethodAndGetResponse(httpClient,httpRequestBase, TestSparkResponse.class);
    }

    @Test
    public void testExecuteHttpMethodAndGetResponse__WhenRecievingInvalidStringResponse_ThenThrowExcpetion() throws Exception {
        Mockito.doReturn("").when(httpClient).execute(Matchers.any(HttpRequestBase.class),Matchers.any(ResponseHandler.class));
        exception.expect(FailedSparkRequestException.class);
        exception.expectMessage("No content to map due to end-of-input");
        HttpRequestUtil.executeHttpMethodAndGetResponse(httpClient, httpRequestBase, TestSparkResponse.class);
    }


    @Test
    public void testExecuteHttpMethodAndGetResponse__WhenRecievingUnmappableStringResponse_ThenThrowExcpetion() throws Exception {
        Mockito.doReturn("{\"action\":\"unknown\"}").when(httpClient).execute(Matchers.any(HttpRequestBase.class),Matchers.any(ResponseHandler.class));
        exception.expect(FailedSparkRequestException.class);
        HttpRequestUtil.executeHttpMethodAndGetResponse(httpClient, httpRequestBase, TestSparkResponse.class);
    }
}