package com.valhalla.holder.mqtt;

import com.valhalla.holder.base.TopicHolderBridge;
import com.valhalla.holder.base.TopicTableMapperProcessor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author : LynX
 * @create 2024/1/8 17:37
 */
@Component
@Slf4j
@Data
public class MqttTopicHolderBridge implements TopicHolderBridge {
    private Boolean mqtt = false;
    private MqttClientService mqttClientService;

    @Autowired
    public void initMqttTopicHolderBridgeBridge(MqttClientService mqttClientService) {
        this.mqttClientService = mqttClientService;
    }

    @Override
    public void init() {
        mqttClientService.clientInit();
    }

    @Override
    public void connectTopicHolder(TopicTableMapperProcessor topic) {
        mqttClientService.subscribe(topic);
    }

    @Override
    public void disconnectTopicHolder(TopicTableMapperProcessor topic) {
        mqttClientService.unsubscribe(topic);
    }
}
