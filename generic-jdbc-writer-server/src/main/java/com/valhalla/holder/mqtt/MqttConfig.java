package com.valhalla.holder.mqtt;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * @author : LynX
 * @create 2024/1/8 17:16
 */
@Configuration("mqttConfig")
@Data
@ConfigurationProperties(prefix = "mqtt")
public class MqttConfig {
    private String host;
    private String clientId;
    private String username;
    private String password;
}
