package com.valhalla.holder.kafka;

import com.valhalla.holder.base.TopicTableMapperHolder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author : LynX
 * @create 2024/1/15 14:14
 */
@Slf4j
@Data
public class KafkaConsumerRunnable implements Runnable {
    private final AtomicBoolean state = new AtomicBoolean(false);
    TopicTableMapperHolder topicTableMapperHolder;
    private KafkaConfig kafkaConfig;
    private String topic;
    private String groupId;

    public KafkaConsumerRunnable(KafkaConfig kafkaConfig, String topic, String groupId, TopicTableMapperHolder topicTableMapperHolder) {
        this.kafkaConfig = kafkaConfig;
        this.topic = topic;
        this.groupId = groupId;
        this.topicTableMapperHolder = topicTableMapperHolder;
    }


    @Override
    public void run() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.getServers());
        if (kafkaConfig.getEnableAutoCommit()) {
            throw new RuntimeException("kafka enableAutoCommit must be false");
        }
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        configs.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, kafkaConfig.getAutoCommitInterval());
        configs.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, kafkaConfig.getMaxPollRecords());
        configs.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, kafkaConfig.getMaxPollInterval());
        configs.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, kafkaConfig.getHeartbeatInterval());
        configs.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, kafkaConfig.getSessionTimeout());
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        configs.put(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG, kafkaConfig.getReconnectBackoff());
        configs.put(ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, kafkaConfig.getMaxReconnectBackoff());

        if (StringUtils.isNotBlank(groupId)) {
            configs.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        } else {
            configs.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaConfig.getGroupId());
        }
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configs);
        try (consumer) {
            consumer.subscribe(Collections.singletonList(topic));
            state.set(true);
            while (state.get()) {
                if (checkKafkaConnection(consumer)) {
                    // 从服务器开始拉取数据
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                    if (records.isEmpty()) {
                        Thread.sleep(kafkaConfig.getCycleTime());
                        continue;
                    } else {
                        boolean b = topicTableMapperHolder.getTopicTableMapperMap().get(topic).getMessageCollectRunnable().getState().get();
                        if (b) {
                            records.forEach(record -> {
                                topicTableMapperHolder.addMessage(topic, record.value());
                            });
                        }
                    }
                    consumer.commitSync();
                } else {
                    log.error("LOG---KafkaConfigRunnable kafka connection is not available, topic: {} ", topic);
                }
            }
        } catch (Exception e) {
            log.error("LOG---KafkaConfigRunnable consume exception: {} ", topic, e);
        }
    }

    private boolean checkKafkaConnection(KafkaConsumer<String, String> consumer) {
        try {
            Map<String, List<PartitionInfo>> topics = consumer.listTopics();
            return topics != null && !topics.isEmpty();
        } catch (TimeoutException e) {
            return false;
        }
    }
}
