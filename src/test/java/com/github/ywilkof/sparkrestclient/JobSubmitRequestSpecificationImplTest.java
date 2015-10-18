package com.github.ywilkof.sparkrestclient;

import org.junit.Assert;
import org.junit.Test;

import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;

/**
 * Created by yonatan on 10.10.15.
 */
public class JobSubmitRequestSpecificationImplTest {

    private SparkRestClient sparkRestClient = SparkRestClient.builder()
            .masterHost("").masterPort(0).build();

    @Test(expected = IllegalArgumentException.class)
    public void tesSubmitJob_WhenAppResourceNotSupplied_ThenThrowException() throws FailedSparkRequestException {
        sparkRestClient
                .prepareJobSubmit()
                .mainClass("com,somepackage.someclass")
                .submit();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSubmitJob_WhenMainClassNotSupplied_ThenThrowException() throws FailedSparkRequestException {
        sparkRestClient
                .prepareJobSubmit()
                .appResource("/path/to/some/file.jar")
                .submit();
    }

    @Test
    public void testJars_WhenMultipleJarsSupplied_ThenReturnCommaSeparatedStringOfJars() {
        final String output = new JobSubmitRequestSpecificationImpl(sparkRestClient).jars("a", Stream.of("b", "c").collect(Collectors.toSet()));
        Assert.assertThat(output, equalTo("a,b,c"));
    }

    @Test
    public void testJars_WhenNullJarsSupplied_ThenReturnOnlyAppResourceJar() {
        final String output = new JobSubmitRequestSpecificationImpl(sparkRestClient).jars("a", null);
        Assert.assertThat(output, equalTo("a"));
    }


}
