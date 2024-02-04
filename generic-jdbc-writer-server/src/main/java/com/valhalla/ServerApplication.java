package com.valhalla;

import com.valhalla.config.ConfigFileReaderConf;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * @author : LynX
 * @create 2024/1/8 15:25
 */
@SpringBootApplication
@Slf4j
public class ServerApplication {
    @Getter
    private static ConfigurableApplicationContext context;

    public static void main(String[] args) {
        context = SpringApplication.run(ServerApplication.class, args);
        loadConf();
        testJdbcDriver();
    }

    private static void loadConf() {
        ConfigFileReaderConf configFileReaderConf = context.getBean(ConfigFileReaderConf.class);
        configFileReaderConf.loadConfigFiles();
    }

    private static void testJdbcDriver() {
        try {
            JdbcTemplate jdbcTemplate = ServerApplication.getContext().getBean(JdbcTemplate.class);
            jdbcTemplate.execute("SELECT 1");
        } catch (Exception e) {
            log.error("JDBC Driver - test failed: {}", e.getMessage());
            throw new RuntimeException(e);
        }
        log.info("JDBC Driver - test success");
    }
}
