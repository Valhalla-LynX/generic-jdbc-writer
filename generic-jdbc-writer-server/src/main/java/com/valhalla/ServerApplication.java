package com.valhalla;

import com.valhalla.config.ConfigFileReaderConf;
import lombok.Getter;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * @author : LynX
 * @create 2024/1/8 15:25
 */
@SpringBootApplication
public class ServerApplication {
    @Getter
    private static ConfigurableApplicationContext context;

    public static void main(String[] args) {
        context = SpringApplication.run(ServerApplication.class, args);
        ConfigFileReaderConf configFileReaderConf = context.getBean(ConfigFileReaderConf.class);
        configFileReaderConf.loadConfigFiles();
    }
}
