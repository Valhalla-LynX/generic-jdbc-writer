package com.valhalla.holder.kafka;

import com.valhalla.holder.base.TopicHolderBridge;
import com.valhalla.holder.base.TopicTableMapperHolder;
import com.valhalla.holder.base.TopicTableMapperProcessor;
import com.valhalla.thread.NamedThreadFactory;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author : LynX
 * @create 2024/1/15 14:14
 */
@Component
@Slf4j
@Data
public class KafkaTopicHolderBridge implements TopicHolderBridge {
    public static final Map<String, ThreadPoolExecutor> KAFKA_CONSUMER_RUNNABLE_POOL = new ConcurrentHashMap<>();
    private final TopicTableMapperHolder topicTableMapperHolder = new TopicTableMapperHolder();
    private Boolean kafka = false;
    private KafkaConfig kafkaConfig;

    @Autowired
    public void initKafkaTopicHolderBridge(KafkaConfig kafkaConfig) {
        this.kafkaConfig = kafkaConfig;
    }

    @Override
    public void init() {

    }

    @Override
    public void connectTopicHolder(TopicTableMapperProcessor topic) {
        topicTableMapperHolder.addMap(topic);
        KafkaConsumerRunnable kafkaConsumerRunnable = new KafkaConsumerRunnable(kafkaConfig, topic.getTopic(), topic.getGroup(), topicTableMapperHolder);
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                1,
                1,
                0L,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(),
                new NamedThreadFactory(topic.getTopic())
        );
        KAFKA_CONSUMER_RUNNABLE_POOL.put(topic.getTopic(), executor);
        executor.execute(kafkaConsumerRunnable);
        log.info("LOG---register KafkaConfigRunnable {} ", topic.getTopic());
    }

    @Override
    public void disconnectTopicHolder(TopicTableMapperProcessor topic) {
        ThreadPoolExecutor executor = KAFKA_CONSUMER_RUNNABLE_POOL.get(topic.getTopic());
        executor.shutdown();
        KAFKA_CONSUMER_RUNNABLE_POOL.remove(topic.getTopic());
        topicTableMapperHolder.removeMap(topic);
        log.info("LOG---unregister KafkaConfigRunnable {} ", topic.getTopic());
    }
}
