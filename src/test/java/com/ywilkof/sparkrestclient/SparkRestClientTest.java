package com.ywilkof.sparkrestclient;

import org.junit.Assert;
import org.junit.Test;

import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;

/**
 * Created by yonatan on 10.10.15.
 */
public class SparkRestClientTest {

    private SparkRestClient sparkRestClient = new SparkRestClient();

    @Test
    public void testJars_WhenMultipleJarsSupplied_ThenReturnCommaSeparatedStringOfJars() {
        final String output = sparkRestClient.jars("a", Stream.of("b", "c").collect(Collectors.toSet()));
        Assert.assertThat(output, equalTo("a,b,c"));
    }

    @Test
    public void testJars_WhenNullJarsSupplied_ThenReturnOnlyAppResourceJar() {
        final String output = sparkRestClient.jars("a", null);
        Assert.assertThat(output, equalTo("a"));
    }


}
