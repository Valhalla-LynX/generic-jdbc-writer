package com.valhalla.holder.base;

import com.valhalla.holder.base.parse.MessageParse2Sql;
import com.valhalla.load.LoadStaticClass;
import com.valhalla.mapper.TopicTableMapper;
import com.valhalla.runnable.MessageCollectRunnable;
import com.valhalla.runnable.MessageParseWriteRunnable;
import com.valhalla.thread.NamedThreadFactory;
import com.valhalla.util.PropertiesUtil;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author : LynX
 * @create 2024/1/9 10:24
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Slf4j
public class TopicTableMapperProcessor extends TopicTableMapper {
    private static final String DEFAULT_PARSE;

    static {
        DEFAULT_PARSE = Objects.requireNonNull(PropertiesUtil.loadProperties("parse.properties")).getProperty("default");
    }

    private final List<String> messageList = Collections.synchronizedList(new ArrayList<>());
    ThreadPoolExecutor messageCollectExecutor = new ThreadPoolExecutor(
            1,
            4,
            0L,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(),
            new NamedThreadFactory(TopicTableMapperProcessor.class.getSimpleName())
    );
    ThreadPoolExecutor messageParseExecutor = new ThreadPoolExecutor(
            1,
            4,
            10L,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(),
            new NamedThreadFactory(TopicTableMapperProcessor.class.getSimpleName())
    );
    private MessageCollectRunnable messageCollectRunnable;
    private MessageParseWriteRunnable messageParseWriteRunnable;

    public TopicTableMapperProcessor(String topic, String table, String columns, String parse) {
        super(topic, table, columns, parse);
        messageParseWriteRunnable = new MessageParseWriteRunnable(this);
        messageCollectRunnable = new MessageCollectRunnable(this, messageParseWriteRunnable);
    }

    public TopicTableMapperProcessor(String topic, String table, String columns, String parse, String share) {
        super(topic, table, columns, parse, share);
        messageParseWriteRunnable = new MessageParseWriteRunnable(this);
        messageCollectRunnable = new MessageCollectRunnable(this, messageParseWriteRunnable);
    }

    public TopicTableMapperProcessor(String topic, String table, String columns, String parse, String share, String group) {
        super(topic, table, columns, parse, null, group);
        messageParseWriteRunnable = new MessageParseWriteRunnable(this);
        messageCollectRunnable = new MessageCollectRunnable(this, messageParseWriteRunnable);
    }

    public static List<TopicTableMapperProcessor> mqttTopicTableMapperArr() {
        Properties properties = PropertiesUtil.loadProperties("mqtt.properties");
        assert properties != null;
        Map<String, Map<String, String>> map = PropertiesUtil.convertByGroup(properties);
        List<TopicTableMapperProcessor> topicTableMapperProcessors = new ArrayList<>();
        map.forEach((k, v) -> {
            String parse = v.get("parse");
            if (parse == null) {
                parse = DEFAULT_PARSE;
            }
            Class<MessageParse2Sql> c = LoadStaticClass.PARSE_CLASS.get(parse);
            TopicTableMapperProcessor p = new TopicTableMapperProcessor(k, v.get("table"), v.get("columns"), parse);
            if (v.containsKey("share")) {
                p.setShare(v.get("share"));
            }
            try {
                p.setMessageParse2Sql(c.getDeclaredConstructor().newInstance());
            } catch (InstantiationException | IllegalAccessException | NoSuchMethodException |
                     InvocationTargetException e) {
                log.error("Parse Class Instance - {} load failed", parse);
            }
            topicTableMapperProcessors.add(p);
            p.getMessageCollectExecutor().execute(p.getMessageCollectRunnable());
            p.getMessageParseExecutor().execute(p.getMessageParseWriteRunnable());
        });
        return topicTableMapperProcessors;
    }

    public static List<TopicTableMapperProcessor> kafkaTopicTableMapperArr() {
        Properties properties = PropertiesUtil.loadProperties("kafka.properties");
        assert properties != null;
        Map<String, Map<String, String>> map = PropertiesUtil.convertByGroup(properties);
        List<TopicTableMapperProcessor> topicTableMapperProcessors = new ArrayList<>();
        map.forEach((k, v) -> {
            String parse = v.get("parse");
            if (parse == null) {
                parse = DEFAULT_PARSE;
            }
            Class<MessageParse2Sql> c = LoadStaticClass.PARSE_CLASS.get(parse);
            TopicTableMapperProcessor p = new TopicTableMapperProcessor(k, v.get("table"), v.get("columns"), parse);
            if (v.containsKey("group")) {
                p.setGroup(v.get("group"));
            }
            try {
                p.setMessageParse2Sql(c.getDeclaredConstructor().newInstance());
            } catch (InstantiationException | IllegalAccessException | NoSuchMethodException |
                     InvocationTargetException e) {
                log.error("Parse Class Instance - {} load failed", parse);
            }
            topicTableMapperProcessors.add(p);
            p.getMessageCollectExecutor().execute(p.getMessageCollectRunnable());
            p.getMessageParseExecutor().execute(p.getMessageParseWriteRunnable());
        });
        return topicTableMapperProcessors;
    }

    public void addMessage(String message) {
        messageList.add(message);
        log.info("add message to topic - {}", message);
    }
}
