package com.valhalla.service;

import com.valhalla.holder.base.TopicHolderBridge;
import com.valhalla.holder.base.TopicTableMapperProcessor;
import com.valhalla.holder.kafka.KafkaTopicHolderBridge;
import com.valhalla.holder.mqtt.MqttTopicHolderBridge;
import com.valhalla.load.LoadStaticClass;
import com.valhalla.util.PropertiesUtil;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.springframework.stereotype.Service;

import java.lang.reflect.InvocationTargetException;

/**
 * @author : LynX
 * @create 2024/1/16 16:45
 */
@Service
@AllArgsConstructor
@Data
public class HolderService {
    public void add(TopicHolderBridge topicHolderBridge, TopicTableMapperProcessor topicTableMapperProcessor, String parse) throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        topicTableMapperProcessor.setMessageParse2Sql(LoadStaticClass.PARSE_CLASS.get(parse).getDeclaredConstructor().newInstance());
        if (topicHolderBridge instanceof KafkaTopicHolderBridge) {
            PropertiesUtil.addMapToProperties("kafka.properties", topicTableMapperProcessor.toPropertiesMap());
        }
        if (topicHolderBridge instanceof MqttTopicHolderBridge) {
            PropertiesUtil.addMapToProperties("mqtt.properties", topicTableMapperProcessor.toPropertiesMap());
        }
        topicHolderBridge.connectTopicHolder(topicTableMapperProcessor);
        if (!topicTableMapperProcessor.getMessageCollectRunnable().getState().get()) {
            topicTableMapperProcessor.getMessageCollectExecutor().execute(topicTableMapperProcessor.getMessageCollectRunnable());
        }
        if (!topicTableMapperProcessor.getMessageParseWriteRunnable().getState().get()) {
            topicTableMapperProcessor.getMessageParseExecutor().execute(topicTableMapperProcessor.getMessageParseWriteRunnable());
        }
        if (topicHolderBridge instanceof KafkaTopicHolderBridge) {
            LoadStaticClass.KAFKA_TABLE_MAPPER_MAP_CLASS.put(topicTableMapperProcessor.getTopic(), topicTableMapperProcessor);
        }
        if (topicHolderBridge instanceof MqttTopicHolderBridge) {
            LoadStaticClass.MQTT_TABLE_MAPPER_MAP_CLASS.put(topicTableMapperProcessor.getTopic(), topicTableMapperProcessor);
        }
    }

    public void start(TopicHolderBridge topicHolderBridge, TopicTableMapperProcessor topicTableMapperProcessor) {
        if (!topicTableMapperProcessor.getMessageCollectRunnable().getState().get()) {
            topicTableMapperProcessor.getMessageCollectExecutor().execute(topicTableMapperProcessor.getMessageCollectRunnable());
        }
        if (!topicTableMapperProcessor.getMessageParseWriteRunnable().getState().get()) {
            topicTableMapperProcessor.getMessageParseExecutor().execute(topicTableMapperProcessor.getMessageParseWriteRunnable());
        }
        topicHolderBridge.connectTopicHolder(topicTableMapperProcessor);
    }

    public void stop(TopicHolderBridge topicHolderBridge, TopicTableMapperProcessor topicTableMapperProcessor) {
        topicHolderBridge.disconnectTopicHolder(topicTableMapperProcessor);
        topicTableMapperProcessor.getMessageCollectRunnable().getState().set(false);
        topicTableMapperProcessor.getMessageParseWriteRunnable().getState().set(false);
    }
}
