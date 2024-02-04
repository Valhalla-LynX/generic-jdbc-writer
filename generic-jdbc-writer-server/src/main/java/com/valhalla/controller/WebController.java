package com.valhalla.controller;

import com.valhalla.config.ConfigFileReaderConf;
import com.valhalla.controller.model.ParseModel;
import com.valhalla.holder.base.TopicTableMapperProcessor;
import com.valhalla.holder.kafka.KafkaTopicHolderBridge;
import com.valhalla.holder.mqtt.MqttTopicHolderBridge;
import com.valhalla.load.LoadStaticClass;
import com.valhalla.service.HolderService;
import com.valhalla.util.PropertiesUtil;
import lombok.AllArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.lang.reflect.InvocationTargetException;
import java.util.*;

/**
 * @author : LynX
 * @create 2024/1/11 19:48
 */
@Controller
@DependsOn({"ConfigFileReaderConf"})
@AllArgsConstructor
public class WebController {
    private MqttTopicHolderBridge mqttTopicHolderBridge;
    private KafkaTopicHolderBridge kafkaTopicHolderBridge;
    private ConfigFileReaderConf configFileReaderConf;
    private HolderService holderService;

    @GetMapping("/")
    public String getIndex() {
        return "index";
    }

    @GetMapping("/kafka")
    public String kafka(Model model) {
        if (!kafkaTopicHolderBridge.getKafka()) {
            model.addAttribute("error", "kafka未开启");
            return "error";
        }
        model.addAttribute("values", LoadStaticClass.KAFKA_TABLE_MAPPER_MAP_CLASS.values());
        return "kafka";
    }

    @GetMapping("/kafka/addPage")
    public String kafkaAddPage() {
        return "kafka_add";
    }

    @GetMapping("/kafka/addPost")
    public String kafkaAddPost(Model model,
                               @RequestParam String topic,
                               @RequestParam String table,
                               @RequestParam String columns,
                               @RequestParam(required = false) String parse,
                               @RequestParam(required = false) String group) throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        if (LoadStaticClass.KAFKA_TABLE_MAPPER_MAP_CLASS.containsKey(topic)) {
            model.addAttribute("error", "topic已存在，请先删除");
            return "error";
        }
        if (StringUtils.isBlank(parse)) {
            parse = LoadStaticClass.DEFAULT_PARSE_CLASS;
        }
        if (StringUtils.isBlank(group)) {
            group = "";
        }
        holderService.add(kafkaTopicHolderBridge, new TopicTableMapperProcessor(topic, table, columns, parse, group), parse);
        model.addAttribute("values", LoadStaticClass.KAFKA_TABLE_MAPPER_MAP_CLASS.values());
        return "redirect:/kafka";
    }

    @GetMapping("/kafka/start")
    public String kafkaStart(Model model, @RequestParam String topic) {
        TopicTableMapperProcessor topicTableMapperProcessor = LoadStaticClass.KAFKA_TABLE_MAPPER_MAP_CLASS.get(topic);
        if (topicTableMapperProcessor == null) {
            model.addAttribute("error", "topic不存在");
            return "error";
        }
        if (topicTableMapperProcessor.getMessageCollectRunnable().getState().get() ||
                topicTableMapperProcessor.getMessageParseWriteRunnable().getState().get()) {
            model.addAttribute("error", "topic正在运行，请先停止");
            return "error";
        }
        holderService.start(kafkaTopicHolderBridge, topicTableMapperProcessor);
        return "redirect:/kafka";
    }

    @GetMapping("/kafka/stop")
    public String kafkaStop(Model model, @RequestParam String topic) {
        TopicTableMapperProcessor topicTableMapperProcessor = LoadStaticClass.KAFKA_TABLE_MAPPER_MAP_CLASS.get(topic);
        if (topicTableMapperProcessor == null) {
            model.addAttribute("error", "topic不存在");
            return "error";
        }
        holderService.stop(kafkaTopicHolderBridge, topicTableMapperProcessor);
        return "redirect:/kafka";
    }

    @GetMapping("/kafka/remove")
    public String kafkaRemove(Model model, @RequestParam String topic) {
        kafkaStop(model, topic);
        Map<String, Map<String, String>> map = PropertiesUtil.convertByGroup(Objects.requireNonNull(PropertiesUtil.loadProperties("kafka.properties")));
        Map<String, String> getMap = map.get(topic);
        if (getMap == null) {
            model.addAttribute("error", "topic不存在");
            return "error";
        }
        Map<String, String> propertiesMap = new HashMap<>();
        getMap.forEach((k, v) -> propertiesMap.put(topic + "." + k, v));
        PropertiesUtil.removeMapFromProperties("kafka.properties", propertiesMap);
        LoadStaticClass.KAFKA_TABLE_MAPPER_MAP_CLASS.remove(topic);
        return "redirect:/kafka";
    }

    @GetMapping("/kafka/reload")
    public String kafkaReload(Model model) {
        Map<String, TopicTableMapperProcessor> topicTableMapperProcessorMap = new HashMap<>(LoadStaticClass.MQTT_TABLE_MAPPER_MAP_CLASS);
        topicTableMapperProcessorMap.forEach((k, v) -> mqttStop(model, k));
        LoadStaticClass.MQTT_TABLE_MAPPER_MAP_CLASS.clear();
        configFileReaderConf.loadConfigFiles();
        return "redirect:/kafka";
    }

    @GetMapping("/mqtt")
    public String mqtt(Model model) {
        if (!mqttTopicHolderBridge.getMqtt()) {
            model.addAttribute("error", "mqtt未开启");
            return "error";
        }
        model.addAttribute("values", LoadStaticClass.MQTT_TABLE_MAPPER_MAP_CLASS.values());
        return "mqtt";
    }

    @GetMapping("/mqtt/addPage")
    public String mqttAddPage() {
        return "mqtt_add";
    }

    @GetMapping("/mqtt/addPost")
    public String mqttAddPost(Model model,
                              @RequestParam String topic,
                              @RequestParam String table,
                              @RequestParam String columns,
                              @RequestParam(required = false) String parse,
                              @RequestParam(required = false) String share) throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        if (LoadStaticClass.MQTT_TABLE_MAPPER_MAP_CLASS.containsKey(topic)) {
            model.addAttribute("error", "topic已存在，请先删除");
            return "error";
        }
        if (StringUtils.isBlank(parse)) {
            parse = LoadStaticClass.DEFAULT_PARSE_CLASS;
        }
        if (StringUtils.isBlank(share)) {
            share = "";
        }
        holderService.add(mqttTopicHolderBridge, new TopicTableMapperProcessor(topic, table, columns, parse, share), parse);
        model.addAttribute("values", LoadStaticClass.MQTT_TABLE_MAPPER_MAP_CLASS.values());
        return "redirect:/mqtt";
    }

    @GetMapping("/mqtt/start")
    public String mqttStart(Model model, @RequestParam String topic) {
        TopicTableMapperProcessor topicTableMapperProcessor = LoadStaticClass.MQTT_TABLE_MAPPER_MAP_CLASS.get(topic);
        if (topicTableMapperProcessor == null) {
            model.addAttribute("error", "topic不存在");
            return "error";
        }
        if (topicTableMapperProcessor.getMessageCollectRunnable().getState().get() ||
                topicTableMapperProcessor.getMessageParseWriteRunnable().getState().get()) {
            model.addAttribute("error", "topic正在运行，请先停止");
            return "error";
        }
        holderService.start(mqttTopicHolderBridge, topicTableMapperProcessor);
        return "redirect:/mqtt";
    }

    @GetMapping("/mqtt/stop")
    public String mqttStop(Model model, @RequestParam String topic) {
        TopicTableMapperProcessor topicTableMapperProcessor = LoadStaticClass.MQTT_TABLE_MAPPER_MAP_CLASS.get(topic);
        if (topicTableMapperProcessor == null) {
            model.addAttribute("error", "topic不存在");
            return "error";
        }
        holderService.stop(mqttTopicHolderBridge, topicTableMapperProcessor);
        return "redirect:/mqtt";
    }

    @GetMapping("/mqtt/remove")
    public String mqttRemove(Model model, @RequestParam String topic) {
        mqttStop(model, topic);
        Map<String, Map<String, String>> map = PropertiesUtil.convertByGroup(Objects.requireNonNull(PropertiesUtil.loadProperties("mqtt.properties")));
        Map<String, String> getMap = map.get(topic);
        if (getMap == null) {
            model.addAttribute("error", "topic不存在");
            return "error";
        }
        Map<String, String> propertiesMap = new HashMap<>();
        getMap.forEach((k, v) -> propertiesMap.put(topic + "." + k, v));
        PropertiesUtil.removeMapFromProperties("mqtt.properties", propertiesMap);
        LoadStaticClass.MQTT_TABLE_MAPPER_MAP_CLASS.remove(topic);
        return "redirect:/mqtt";
    }

    @GetMapping("/mqtt/reload")
    public String mqttReload(Model model) {
        Map<String, TopicTableMapperProcessor> topicTableMapperProcessorMap = new HashMap<>(LoadStaticClass.MQTT_TABLE_MAPPER_MAP_CLASS);
        topicTableMapperProcessorMap.forEach((k, v) -> mqttStop(model, k));
        LoadStaticClass.MQTT_TABLE_MAPPER_MAP_CLASS.clear();
        configFileReaderConf.loadConfigFiles();
        return "redirect:/mqtt";
    }

    @GetMapping("/parse")
    public String parse(Model model) {
        Collection<ParseModel> list = new ArrayList<>();
        LoadStaticClass.PARSE_CLASS.forEach((k, v) -> {
            ParseModel pm;
            String rule = null;
            try {
                rule = v.getConstructor().newInstance().getParseRule();
                pm = new ParseModel(k, rule);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            list.add(pm);
        });
        model.addAttribute("list", list);
        return "parse";
    }
}
