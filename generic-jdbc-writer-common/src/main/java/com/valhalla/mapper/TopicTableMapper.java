package com.valhalla.mapper;

import com.valhalla.holder.base.parse.MessageParse2Sql;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.Map;

/**
 * @author : LynX
 * @create 2024/1/8 17:21
 */
@Data
@NoArgsConstructor
public class TopicTableMapper {
    private String topic;
    private String table;
    private String columns;
    private String parse;
    private String share;
    private String group;
    private MessageParse2Sql messageParse2Sql;

    public TopicTableMapper(String topic, String table, String columns, String parse) {
        this.topic = topic;
        this.table = table;
        this.columns = columns;
        this.parse = parse;
    }

    public TopicTableMapper(String topic, String table, String columns, String parse, String share) {
        this.topic = topic;
        this.table = table;
        this.columns = columns;
        this.parse = parse;
        this.share = share;
    }

    public TopicTableMapper(String topic, String table, String columns, String parse, String share, String group) {
        this.topic = topic;
        this.table = table;
        this.columns = columns;
        this.parse = parse;
        this.group = group;
    }

    public Map<String, String> toPropertiesMap() {
        Map<String, String> map = new HashMap<>();
        map.put(this.topic + ".table", this.table);
        map.put(this.topic + ".columns", this.columns);
        map.put(this.topic + ".parse", this.parse);
        map.put(this.topic + ".share", this.share);
        return map;
    }

    public String getMessageParse2SqlName() {
        return this.messageParse2Sql.getClass().getSimpleName();
    }
}
