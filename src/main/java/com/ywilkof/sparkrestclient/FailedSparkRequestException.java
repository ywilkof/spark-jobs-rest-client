package com.ywilkof.sparkrestclient;

/**
 * Created by yonatan on 08.10.15.
 */
public final class FailedSparkRequestException extends Exception {

    public FailedSparkRequestException(String message) {
        super(message);
    }

    public FailedSparkRequestException(Throwable cause) {
        super(cause);
    }
}
