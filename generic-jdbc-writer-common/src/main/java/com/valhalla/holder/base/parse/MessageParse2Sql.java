package com.valhalla.holder.base.parse;

import java.util.List;

/**
 * @author : LynX
 * @create 2024/1/9 15:12
 */
public interface MessageParse2Sql {
    String INSERT = "INSERT INTO %s (%s) VALUES %s;";

    String parseToSql(String table, String columns, List<String> messages);

    default String getParseRule() {
        return "";
    }
}
