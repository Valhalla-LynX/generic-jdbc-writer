package com.valhalla.config;

import lombok.AllArgsConstructor;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * @author : LynX
 * @create 2024/1/11 16:51
 */
@Configuration
@AllArgsConstructor
public class JdbcConf {
    private final JdbcTemplate jdbcTemplate;

    public static void insertData(JdbcTemplate jdbcTemplate, String sql, Object... args) {
        jdbcTemplate.update(sql, args);
    }
}