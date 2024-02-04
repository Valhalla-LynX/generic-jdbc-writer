package com.valhalla.holder.mqtt;

import com.valhalla.holder.base.TopicTableMapperProcessor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;

/**
 * @author : LynX
 * @create 2024/1/8 17:43
 */
@Component
@Slf4j
public class MqttClientService {
    private MqttConfig config;
    private MqttCallback callback;
    private MqttClient client;

    @Autowired
    @DependsOn({"mqttConfig", "mqttCallback"})
    private void intiMqttService(MqttConfig mqttConfig, MqttCallback mqttCallback) {
        this.config = mqttConfig;
        mqttCallback.setMqttClientService(this);
        this.callback = mqttCallback;
    }

    protected void clientInit() {
        if (config.getHost() == null || config.getHost().isEmpty()) {
            log.info("LOG---without mqtt connection");
        } else {
            log.info("LOG---init mqtt connection");
            clientConnect();
        }
    }

    private void clientConnect() {
        try {
            MqttConnectOptions options = new MqttConnectOptions();
            String[] uri = new String[1];
            uri[0] = config.getHost();
            options.setServerURIs(uri);
            if (config.getUsername() != null) {
                options.setUserName(config.getUsername());
            }
            if (config.getPassword() != null) {
                options.setPassword(config.getPassword().toCharArray());
            }
            options.setAutomaticReconnect(true);
            options.setMaxReconnectDelay(3000);
            options.setCleanSession(true);
            client = new MqttClient(config.getHost(), config.getClientId() + "-" + System.currentTimeMillis() % 1000, new MemoryPersistence());
            client.setCallback(callback);
            client.connect(options);
        } catch (MqttException me) {
            me.printStackTrace();
            log.error("LOG---connect to mqtt error: {}", me.getMessage());
        }
    }

    public void subscribe(TopicTableMapperProcessor topicTableMapper) {
        try {
            callback.addMap(topicTableMapper);
            if (StringUtils.isNotBlank(topicTableMapper.getShare())) {
                client.subscribe(topicTableMapper.getShare(), 0);
            } else {
                client.subscribe(topicTableMapper.getTopic(), 0);
            }
            log.info("LOG---subscribe topic: {}", topicTableMapper.getTopic());
        } catch (MqttException me) {
            log.error("LOG---subscribe error: {}", me.getMessage());
        }
    }

    public void unsubscribe(TopicTableMapperProcessor topicTableMapper) {
        try {
            client.unsubscribe(topicTableMapper.getTopic());
            callback.removeMap(topicTableMapper);
            log.info("LOG---unsubscribe topic: {}", topicTableMapper.getTopic());
        } catch (MqttException me) {
            log.error("LOG---unsubscribe error: {}", me.getMessage());
        }
    }
}
