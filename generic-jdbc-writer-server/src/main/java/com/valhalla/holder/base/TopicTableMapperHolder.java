package com.valhalla.holder.base;

import lombok.Data;

import java.util.HashMap;
import java.util.Map;

/**
 * @author : LynX
 * @create 2024/1/8 18:14
 */
@Data
public class TopicTableMapperHolder {
    private final Map<String, TopicTableMapperProcessor> topicTableMapperMap = new HashMap<>();

    public void addMap(TopicTableMapperProcessor topicTableMapper) {
        topicTableMapperMap.put(topicTableMapper.getTopic(), topicTableMapper);
    }

    public void removeMap(TopicTableMapperProcessor topicTableMapper) {
        topicTableMapperMap.remove(topicTableMapper.getTopic());
    }

    public void addMessage(String topic, String message) {
        topicTableMapperMap.get(topic).addMessage(message);
    }
}
