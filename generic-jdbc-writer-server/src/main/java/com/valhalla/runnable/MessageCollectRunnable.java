package com.valhalla.runnable;

import com.valhalla.holder.base.TopicTableMapperProcessor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author : LynX
 * @create 2024/1/9 15:06
 */
@Data
@Slf4j
public class MessageCollectRunnable implements Runnable {
    private final AtomicBoolean state = new AtomicBoolean(false);
    private final TopicTableMapperProcessor topicTableMapperProcessor;
    private final MessageParseWriteRunnable messageParseWriteRunnable;

    public MessageCollectRunnable(TopicTableMapperProcessor topicTableMapperProcessor, MessageParseWriteRunnable messageParseWriteRunnable) {
        this.topicTableMapperProcessor = topicTableMapperProcessor;
        this.messageParseWriteRunnable = messageParseWriteRunnable;
    }

    @Override
    public void run() {
        state.set(true);
        long lastProcessTime = System.currentTimeMillis();
        List<String> messageList = topicTableMapperProcessor.getMessageList();
        while (state.get()) {
            synchronized (messageList) {
                boolean overload = messageList.size() >= 500;
                if ((overload || System.currentTimeMillis() - lastProcessTime >= 500) && !messageParseWriteRunnable.getWriting().get()) {
                    if (!messageList.isEmpty()) {
                        messageParseWriteRunnable.getCopyList().addAll(messageList);
                        messageList.clear();
                        messageParseWriteRunnable.getWriting().set(true);
                    }
                    lastProcessTime = System.currentTimeMillis();
                } else {
                    log.debug("MessageCollectRunnable is not ready");
                }
            }
            try {
                // Sleep for a short time to avoid busy waiting
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
}
