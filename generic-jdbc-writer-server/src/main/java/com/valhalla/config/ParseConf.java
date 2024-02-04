package com.valhalla.config;

import com.valhalla.holder.base.parse.MessageParse2Sql;
import com.valhalla.load.DynamicClassLoader;
import com.valhalla.load.LoadStaticClass;
import com.valhalla.util.PropertiesUtil;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Configuration;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @author : LynX
 * @create 2024/1/9 16:29
 */
@Configuration("ParseConf")
@Slf4j
public class ParseConf {

    private static DynamicClassLoader getDynamicClassLoader(List<String> values, int i) {
        String v = values.get(i);
        String parseJarPath = "./parse/" + v;
        // 创建URL
        URL parseUrl;
        try {
            // 引用common模块
            parseUrl = new URL("file:" + parseJarPath);
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
        // 创建URLClassLoader
        ClassLoader parent = Thread.currentThread().getContextClassLoader();
        return new DynamicClassLoader(new URL[]{parseUrl}, parent);
    }

    @PostConstruct
    public void loadParseDriver() {
        Properties properties = PropertiesUtil.loadProperties("parse.properties");
        if (properties != null) {
            Map<String, String> map = PropertiesUtil.convert(properties);
            List<String> keys = map.keySet().stream().toList();
            List<String> values = map.values().stream().toList();
            for (int i = 0; i < map.size(); i++) {
                String k = keys.get(i);
                DynamicClassLoader urlDynamicClassLoader = getDynamicClassLoader(values, i);
                // 设置系统的类加载器
                Thread.currentThread().setContextClassLoader(urlDynamicClassLoader);
                // 加载驱动程序
                try {
                    Class<MessageParse2Sql> aClass = (Class<MessageParse2Sql>) urlDynamicClassLoader.loadClass(k);
                    LoadStaticClass.PARSE_CLASS.put(k, aClass);
                    if (LoadStaticClass.DEFAULT_PARSE_CLASS == null) {
                        LoadStaticClass.DEFAULT_PARSE_CLASS = k;
                    }
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException(e);
                }

                log.info("Parse Driver - {} loaded", k);
            }
        }
    }
}
