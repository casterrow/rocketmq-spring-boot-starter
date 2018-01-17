package com.github.rocketmq.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;


@Data
@ConfigurationProperties(prefix = "rocketmq")
public class MQProperties {
    /**
     * config name server address
     */
    private String nameServerAddress;
    /**
     * config producer group , default to DPG+RANDOM UUID like DPG-fads-3143-123d-1111
     */
    private String producerGroup;
    /**
     * config send message timeout
     */
    private Integer sendMsgTimeout = 3000;
    /**
     * switch of trace message consumer: send message consumer info to topic: MQ_TRACE_DATA
     */
    private Boolean traceEnabled = Boolean.FALSE;
}
