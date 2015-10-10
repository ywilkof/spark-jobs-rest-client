package com.ywilkof.sparkrestclient;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Getter;
import lombok.Setter;

/**
 * Created by yonatan on 08.10.15.
 */
@Getter
@Setter
@JsonIgnoreProperties(ignoreUnknown = true)
class JobSubmitResponse extends AbstractSparkResponse{

    JobSubmitResponse() {}

    private String action;
}
