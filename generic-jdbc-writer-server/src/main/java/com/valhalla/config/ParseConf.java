package com.valhalla.config;

import com.valhalla.holder.base.parse.MessageParse2Sql;
import com.valhalla.load.LoadStaticClass;
import com.valhalla.util.PropertiesUtil;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Configuration;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * @author : LynX
 * @create 2024/1/9 16:29
 */
@Configuration("ParseConf")
@Slf4j
public class ParseConf {

    @PostConstruct
    public void loadParseDriver() {
        Properties properties = PropertiesUtil.loadProperties("parse.properties");
        if (properties != null) {
            Map<String, String> map = PropertiesUtil.convert(properties);
            Set<Map.Entry<String, String>> keySet = map.entrySet();
            List<String> keys = map.keySet().stream().toList();
            List<String> values = map.values().stream().toList();
            for (int i = 0; i < map.size(); i++) {
                String k = keys.get(i);
                String v = values.get(i);
                String parseJarPath = "./parse/" + v;
                // 创建URL
                URL parseUrl;
                try {
                    parseUrl = new URL("file:" + parseJarPath);
                } catch (MalformedURLException e) {
                    throw new RuntimeException(e);
                }
                // 创建URLClassLoader
                URLClassLoader urlClassLoader = new URLClassLoader(new URL[]{parseUrl});
                // 设置系统的类加载器
                Thread.currentThread().setContextClassLoader(urlClassLoader);
                // 加载驱动程序
                try {
                    Class<MessageParse2Sql> aClass = (Class<MessageParse2Sql>) urlClassLoader.loadClass(k);
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
