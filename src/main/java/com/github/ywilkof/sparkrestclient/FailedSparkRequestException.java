package com.github.ywilkof.sparkrestclient;

import lombok.Getter;

/**
 * Created by yonatan on 08.10.15.
 */
public final class FailedSparkRequestException extends Exception {

    @Getter
    private DriverState state;
    public FailedSparkRequestException(String message) {
        super(message);
    }

    public FailedSparkRequestException(String message, DriverState driverstate) {
        super(message);
        this.state = driverstate;
    }

    public FailedSparkRequestException(Throwable cause) {
        super(cause);
    }
}
