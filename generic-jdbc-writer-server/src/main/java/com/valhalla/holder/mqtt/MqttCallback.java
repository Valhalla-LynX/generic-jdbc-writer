package com.valhalla.holder.mqtt;

import com.valhalla.holder.base.TopicTableMapperHolder;
import com.valhalla.holder.base.TopicTableMapperProcessor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.springframework.stereotype.Component;

/**
 * @author : LynX
 * @create 2024/1/8 17:10
 */
@Component("mqttCallback")
@Slf4j
@Data
public class MqttCallback implements MqttCallbackExtended {
    private final TopicTableMapperHolder topicTableMapperHolder = new TopicTableMapperHolder();
    private MqttClientService mqttClientService;

    @Override
    public void connectComplete(boolean b, String s) {
        if (b) {
            log.info("LOG---reconnect to mqtt: success: {}", s);
            if (mqttClientService != null) {
                topicTableMapperHolder.getTopicTableMapperMap().forEach((k, v) -> {
                    mqttClientService.subscribe(v);
                });
            }
        } else {
            log.info("LOG---connect to mqtt: success: {}", s);
        }
    }

    @Override
    public void connectionLost(Throwable throwable) {
        log.error("LOG---lost connection with mqtt: " + throwable.getMessage());
    }

    @Override
    public void messageArrived(String s, MqttMessage mqttMessage) {
        log.debug("LOG---message arrived: topic: {}, message: {}", s, mqttMessage.toString());
        boolean b = topicTableMapperHolder.getTopicTableMapperMap().get(s).getMessageCollectRunnable().getState().get();
        if (b) {
            topicTableMapperHolder.addMessage(s, mqttMessage.toString());
        }
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {
    }

    public void addMap(TopicTableMapperProcessor topicTableMappers) {
        topicTableMapperHolder.addMap(topicTableMappers);
    }

    public void removeMap(TopicTableMapperProcessor topicTableMappers) {
        topicTableMapperHolder.removeMap(topicTableMappers);
    }
}
