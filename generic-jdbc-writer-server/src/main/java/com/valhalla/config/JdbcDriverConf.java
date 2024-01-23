package com.valhalla.config;

import com.valhalla.util.PropertiesUtil;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Map;
import java.util.Properties;

/**
 * @author : LynX
 * @create 2024/1/8 16:43
 */
@Configuration("JdbcDriverConf")
@Slf4j
@DependsOn({"ParseConf"})
public class JdbcDriverConf {
    @Value("${spring.datasource.driverClassName}")
    private String driverClassName;

    @PostConstruct
    public void loadJdbcDriver() throws MalformedURLException, ClassNotFoundException {
        Properties properties = PropertiesUtil.loadProperties("driver.properties");
        if (properties != null) {
            Map<String, String> map = PropertiesUtil.convert(properties);
            if (map.containsKey(driverClassName)) {
                String driverJarPath = map.get(driverClassName);
                // JDBC驱动程序的JAR文件路径
                String mysqlJdbcJarPath = "./driver/" + driverJarPath;
                // 创建URL
                URL mysqlJdbcUrl = new URL("file:" + mysqlJdbcJarPath);
                // 创建URLClassLoader
                URLClassLoader urlClassLoader = new URLClassLoader(new URL[]{mysqlJdbcUrl});
                // 设置系统的类加载器
                Thread.currentThread().setContextClassLoader(urlClassLoader);
                // 加载驱动程序
                Class.forName(driverClassName, true, urlClassLoader);
                log.info("JDBC Driver - {} loaded", driverClassName);
            }
        }
    }
}
