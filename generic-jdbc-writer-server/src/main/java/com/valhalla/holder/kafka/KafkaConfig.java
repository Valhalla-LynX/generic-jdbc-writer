package com.valhalla.holder.kafka;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * @author : LynX
 * @create 2024/1/15 14:47
 */
@Configuration
@Data
@ConfigurationProperties(prefix = "kafka")
public class KafkaConfig {
    private String servers;
    private String groupId;
    private Boolean enableAutoCommit;
    private Integer autoCommitInterval;
    private Boolean autoOffsetReset;
    private Integer maxPollRecords;
    private Integer cycleTime;
    private Integer maxPollInterval;
    private Integer heartbeatInterval;
    private Integer sessionTimeout;
    private Integer reconnectBackoff;
    private Integer maxReconnectBackoff;
}
