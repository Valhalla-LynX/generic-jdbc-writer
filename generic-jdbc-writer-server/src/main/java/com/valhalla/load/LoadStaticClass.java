package com.valhalla.load;

import com.valhalla.holder.base.TopicTableMapperProcessor;
import com.valhalla.holder.base.parse.MessageParse2Sql;

import java.util.HashMap;
import java.util.Map;

/**
 * @author : LynX
 * @create 2024/1/10 11:35
 */
public class LoadStaticClass {
    public static final Map<String, Class<MessageParse2Sql>> PARSE_CLASS = new HashMap<>();
    public static final Map<String, TopicTableMapperProcessor> MQTT_TABLE_MAPPER_MAP_CLASS = new HashMap<>();
    public static final Map<String, TopicTableMapperProcessor> KAFKA_TABLE_MAPPER_MAP_CLASS = new HashMap<>();
    public static String DEFAULT_PARSE_CLASS;
}
