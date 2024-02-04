package com.valhalla.holder.base.parse;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * @author : LynX
 * @create 2024/1/9 15:19
 */
@NoArgsConstructor
public class JsonStructMessageParse2Sql implements MessageParse2Sql {
    @Override
    public String parseToSql(String table, String columns, List<String> messages) {
        StringBuilder valueSb = new StringBuilder();
        messages.forEach(message -> {
            valueSb.append(parseJson(message));
            valueSb.append(",");
        });
        valueSb.deleteCharAt(valueSb.length() - 1);
        return String.format(INSERT, table, columns, valueSb);
    }

    private String parseJson(String message) {
        JSONObject jo = JSONObject.parseObject(message);
        String type = jo.getString("type");
        JSONArray recordJa = jo.getJSONArray("data");
        switch (type) {
            case "single":
                // ["name",1]
                String str = recordJa.toJSONString().replace("\"", "'");
                return "(" + str.substring(1, str.length() - 1) + ")";
            case "multiple":
                // [["name",1],["name",1]]
                String strMultiple = recordJa.toJSONString();
                strMultiple = strMultiple.substring(1, strMultiple.length() - 1);
                strMultiple = strMultiple.replace('\"', '\'');
                strMultiple = strMultiple.replace('[', '(');
                strMultiple = strMultiple.replace('[', ')');
                return strMultiple;
            default:
                return null;
        }
    }

    @Override
    public String getParseRule() {
        return """
                single input: {
                  "type": "single",
                  "table": "test",
                  "data": [0,"test"]
                }
                <br/>
                single output: (0,"test")
                <br/>
                multiple input: {
                  "type": "multiple",
                  "table": "test",
                  "data": [[0,"test"],[0,"test"]]
                }
                <br/>
                multiple output: (0,"test"),(0,"test")
                """;
    }
}
