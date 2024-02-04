package com.valhalla.config;

import com.valhalla.holder.base.TopicTableMapperProcessor;
import com.valhalla.holder.kafka.KafkaTopicHolderBridge;
import com.valhalla.holder.mqtt.MqttTopicHolderBridge;
import com.valhalla.load.LoadStaticClass;
import com.valhalla.util.PropertiesUtil;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @author : LynX
 * @create 2024/1/8 15:30
 */
@Configuration("ConfigFileReaderConf")
@DependsOn({"JdbcDriverConf", "ParseConf"})
@AllArgsConstructor
public class ConfigFileReaderConf {
    @Getter
    private static Long cycle = 1000L;
    @Getter
    private static Long list = 1000L;
    private MqttTopicHolderBridge mqttTopicHolderBridge;
    private KafkaTopicHolderBridge kafkaTopicHolderBridge;

    public void loadConfigFiles() {
        Properties properties = PropertiesUtil.loadProperties("server.properties");
        loadReceiverProperties(properties);
    }

    private void loadReceiverProperties(Properties properties) {
        Map<String, String> map = PropertiesUtil.convert(properties);
        if (map.containsKey("mqtt") && "true".equals(map.get("mqtt"))) {
            mqttTopicHolderBridge.setMqtt(true);
            mqttTopicHolderBridge.init();
            loadMqttConf();
        }
        if (map.containsKey("kafka") && "true".equals(map.get("kafka"))) {
            kafkaTopicHolderBridge.setKafka(true);
            kafkaTopicHolderBridge.init();
            loadKafkaConf();
        }
        if (map.containsKey("cycle")) {
            cycle = Long.parseLong(map.get("cycle"));
        }
        if (map.containsKey("list")) {
            list = Long.parseLong(map.get("list"));
        }
    }

    //读取mqtt配置文件，加载到mqttHolder中，mqttHolder会自动订阅
    private void loadMqttConf() {
        List<TopicTableMapperProcessor> list = TopicTableMapperProcessor.mqttTopicTableMapperArr();
        for (TopicTableMapperProcessor topicTableMapperProcessor : list) {
            mqttTopicHolderBridge.connectTopicHolder(topicTableMapperProcessor);
            LoadStaticClass.MQTT_TABLE_MAPPER_MAP_CLASS.put(topicTableMapperProcessor.getTopic(), topicTableMapperProcessor);
        }
    }

    private void loadKafkaConf() {
        List<TopicTableMapperProcessor> list = TopicTableMapperProcessor.kafkaTopicTableMapperArr();
        for (TopicTableMapperProcessor topicTableMapperProcessor : list) {
            kafkaTopicHolderBridge.connectTopicHolder(topicTableMapperProcessor);
            LoadStaticClass.KAFKA_TABLE_MAPPER_MAP_CLASS.put(topicTableMapperProcessor.getTopic(), topicTableMapperProcessor);
        }
    }
}
