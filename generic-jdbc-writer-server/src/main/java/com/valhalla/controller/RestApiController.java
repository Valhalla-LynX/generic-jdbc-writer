package com.valhalla.controller;

import com.alibaba.fastjson2.JSONObject;
import com.valhalla.config.ConfigFileReaderConf;
import com.valhalla.controller.model.R;
import com.valhalla.holder.base.TopicTableMapperProcessor;
import com.valhalla.holder.kafka.KafkaTopicHolderBridge;
import com.valhalla.holder.mqtt.MqttTopicHolderBridge;
import com.valhalla.load.LoadStaticClass;
import com.valhalla.service.HolderService;
import com.valhalla.util.PropertiesUtil;
import lombok.AllArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.springframework.web.bind.annotation.*;

import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * @author : LynX
 * @create 2024/1/8 17:06
 */
@RestController
@RequestMapping("/api")
@AllArgsConstructor
public class RestApiController {
    private MqttTopicHolderBridge mqttTopicHolderBridge;
    private KafkaTopicHolderBridge kafkaTopicHolderBridge;
    private ConfigFileReaderConf configFileReaderConf;
    private HolderService holderService;

    @GetMapping("/ping")
    public String ping() {
        return "pong";
    }

    @GetMapping("/kafka")
    public R<Collection<TopicTableMapperProcessor>> kafka() {
        if (!kafkaTopicHolderBridge.getKafka()) {
            return R.error("kafka未开启");
        }
        return R.ok(LoadStaticClass.KAFKA_TABLE_MAPPER_MAP_CLASS.values());
    }

    @PostMapping("/kafka/addPost")
    public R<String> kafkaAddPost(@RequestBody JSONObject jo) throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        String topic = jo.getString("topic");
        String table = jo.getString("table");
        String columns = jo.getString("columns");
        String parse = jo.getString("parse");
        String group = jo.getString("group");
        if (StringUtils.isBlank(topic) || StringUtils.isBlank(table) || StringUtils.isBlank(columns)) {
            return R.error("参数不能为空");
        }
        if (LoadStaticClass.KAFKA_TABLE_MAPPER_MAP_CLASS.containsKey(topic)) {
            return R.error("topic已存在");
        }
        if (StringUtils.isBlank(parse)) {
            parse = LoadStaticClass.DEFAULT_PARSE_CLASS;
        }
        if (StringUtils.isBlank(group)) {
            group = "";
        }
        holderService.add(kafkaTopicHolderBridge, new TopicTableMapperProcessor(topic, table, columns, parse, group), parse);
        return R.ok("添加成功");
    }

    @GetMapping("/kafka/start")
    public R<String> kafkaStart(@RequestParam String topic) {
        TopicTableMapperProcessor topicTableMapperProcessor = LoadStaticClass.KAFKA_TABLE_MAPPER_MAP_CLASS.get(topic);
        if (topicTableMapperProcessor == null) {
            return R.error("topic不存在");
        }
        if (topicTableMapperProcessor.getMessageCollectRunnable().getState().get() ||
                topicTableMapperProcessor.getMessageParseWriteRunnable().getState().get()) {
            return R.error("topic已启动");
        }
        holderService.start(kafkaTopicHolderBridge, topicTableMapperProcessor);
        return R.ok("启动成功");
    }

    @GetMapping("/kafka/stop")
    public R<String> kafkaStop(@RequestParam String topic) {
        TopicTableMapperProcessor topicTableMapperProcessor = LoadStaticClass.KAFKA_TABLE_MAPPER_MAP_CLASS.get(topic);
        if (topicTableMapperProcessor == null) {
            return R.error("topic不存在");
        }
        if (!topicTableMapperProcessor.getMessageCollectRunnable().getState().get() ||
                !topicTableMapperProcessor.getMessageParseWriteRunnable().getState().get()) {
            return R.error("topic已停止");
        }
        holderService.stop(kafkaTopicHolderBridge, topicTableMapperProcessor);
        return R.ok("停止成功");
    }

    @GetMapping("/kafka/remove")
    public R<String> kafkaRemove(@RequestParam String topic) {
        kafkaStop(topic);
        Map<String, Map<String, String>> map = PropertiesUtil.convertByGroup(Objects.requireNonNull(PropertiesUtil.loadProperties("kafka.properties")));
        Map<String, String> getMap = map.get(topic);
        if (getMap == null) {
            return R.error("topic不存在");
        }
        Map<String, String> propertiesMap = new HashMap<>();
        getMap.forEach((k, v) -> propertiesMap.put(topic + "." + k, v));
        PropertiesUtil.removeMapFromProperties("kafka.properties", propertiesMap);
        LoadStaticClass.KAFKA_TABLE_MAPPER_MAP_CLASS.remove(topic);
        return R.ok("删除成功");
    }

    @GetMapping("/mqtt")
    public R<Collection<TopicTableMapperProcessor>> mqtt() {
        if (!mqttTopicHolderBridge.getMqtt()) {
            return R.error("mqtt未开启");
        }
        return R.ok(LoadStaticClass.MQTT_TABLE_MAPPER_MAP_CLASS.values());
    }

    @PostMapping("/mqtt/addPost")
    public R<String> mqttAddPost(@RequestBody JSONObject jo) throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        String topic = jo.getString("topic");
        String table = jo.getString("table");
        String columns = jo.getString("columns");
        String parse = jo.getString("parse");
        String share = jo.getString("share");
        if (StringUtils.isBlank(topic) || StringUtils.isBlank(table) || StringUtils.isBlank(columns)) {
            return R.error("参数不能为空");
        }
        if (LoadStaticClass.MQTT_TABLE_MAPPER_MAP_CLASS.containsKey(topic)) {
            return R.error("topic已存在");
        }
        if (StringUtils.isBlank(parse)) {
            parse = LoadStaticClass.DEFAULT_PARSE_CLASS;
        }
        if (StringUtils.isBlank(share)) {
            share = "";
        }
        holderService.add(mqttTopicHolderBridge, new TopicTableMapperProcessor(topic, table, columns, parse, share), parse);
        return R.ok("添加成功");
    }

    @GetMapping("/mqtt/start")
    public R<String> mqttStart(@RequestParam String topic) {
        TopicTableMapperProcessor topicTableMapperProcessor = LoadStaticClass.MQTT_TABLE_MAPPER_MAP_CLASS.get(topic);
        if (topicTableMapperProcessor == null) {
            return R.error("topic不存在");
        }
        if (topicTableMapperProcessor.getMessageCollectRunnable().getState().get() ||
                topicTableMapperProcessor.getMessageParseWriteRunnable().getState().get()) {
            return R.error("topic已启动");
        }
        holderService.start(mqttTopicHolderBridge, topicTableMapperProcessor);
        return R.ok("启动成功");
    }

    @GetMapping("/mqtt/stop")
    public R<String> mqttStop(@RequestParam String topic) {
        TopicTableMapperProcessor topicTableMapperProcessor = LoadStaticClass.MQTT_TABLE_MAPPER_MAP_CLASS.get(topic);
        if (topicTableMapperProcessor == null) {
            return R.error("topic不存在");
        }
        if (!topicTableMapperProcessor.getMessageCollectRunnable().getState().get() ||
                !topicTableMapperProcessor.getMessageParseWriteRunnable().getState().get()) {
            return R.error("topic已停止");
        }
        holderService.stop(mqttTopicHolderBridge, topicTableMapperProcessor);
        return R.ok("停止成功");
    }

    @GetMapping("/mqtt/remove")
    public R<String> mqttRemove(@RequestParam String topic) {
        mqttStop(topic);
        Map<String, Map<String, String>> map = PropertiesUtil.convertByGroup(Objects.requireNonNull(PropertiesUtil.loadProperties("mqtt.properties")));
        Map<String, String> getMap = map.get(topic);
        if (getMap == null) {
            return R.error("topic不存在");
        }
        Map<String, String> propertiesMap = new HashMap<>();
        getMap.forEach((k, v) -> propertiesMap.put(topic + "." + k, v));
        PropertiesUtil.removeMapFromProperties("mqtt.properties", propertiesMap);
        LoadStaticClass.MQTT_TABLE_MAPPER_MAP_CLASS.remove(topic);
        return R.ok("删除成功");
    }

    @GetMapping("/mqtt/reload")
    public R<String> mqttReload() {
        Map<String, TopicTableMapperProcessor> topicTableMapperProcessorMap = new HashMap<>(LoadStaticClass.MQTT_TABLE_MAPPER_MAP_CLASS);
        topicTableMapperProcessorMap.forEach((k, v) -> mqttStop(k));
        LoadStaticClass.MQTT_TABLE_MAPPER_MAP_CLASS.clear();
        configFileReaderConf.loadConfigFiles();
        return R.ok("重载成功");
    }
}
