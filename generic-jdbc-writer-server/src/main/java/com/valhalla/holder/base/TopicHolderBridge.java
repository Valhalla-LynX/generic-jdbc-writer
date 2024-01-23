package com.valhalla.holder.base;

/**
 * @author : LynX
 * @create 2024/1/8 17:39
 */
public interface TopicHolderBridge {
    void init();

    void connectTopicHolder(TopicTableMapperProcessor topic);

    void disconnectTopicHolder(TopicTableMapperProcessor topic);
}
