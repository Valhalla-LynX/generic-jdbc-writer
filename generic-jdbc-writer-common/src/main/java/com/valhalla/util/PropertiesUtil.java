package com.valhalla.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * @author : LynX
 * @create 2024/1/8 15:54
 */
public class PropertiesUtil {
    public static Map<String, String> convert(Properties properties) {
        return properties.entrySet().stream()
                .collect(Collectors.toMap(e -> e.getKey().toString(), e -> e.getValue().toString()));
    }

    public static Map<String, Map<String, String>> convertByGroup(Properties properties) {
        return properties.entrySet().stream()
                .collect(Collectors.groupingBy(e -> e.getKey().toString().split("\\.")[0],
                        Collectors.toMap(e -> e.getKey().toString().split("\\.")[1], e -> e.getValue().toString())));
    }

    public static Properties loadProperties(String name) {
        Path path = Paths.get("./conf", name);
        Properties properties = new Properties();
        try {
            properties.load(Files.newInputStream(path));
            return properties;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static void writeProperties(String name, Map<String, String> orderedMap) {
        Path path = Paths.get("./conf", name);
        File file = path.toFile();

        try (FileOutputStream fos = new FileOutputStream(file)) {
            for (Map.Entry<String, String> entry : orderedMap.entrySet()) {
                if (entry.getValue() != null) {
                    fos.write((entry.getKey() + "=" + entry.getValue() + "\n").getBytes());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void addMapToProperties(String name, Map<String, String> map) {
        Map<String, String> properties = convert(Objects.requireNonNull(loadProperties(name)));
        properties.putAll(map);
        Map<String, String> orderedMap = new TreeMap<>(properties);
        writeProperties(name, orderedMap);
    }

    public static void removeMapFromProperties(String name, Map<String, String> map) {
        Map<String, String> propertiesMap = convert(Objects.requireNonNull(loadProperties(name)));
        propertiesMap.keySet().removeAll(map.keySet());
        Map<String, String> orderedMap = new TreeMap<>(propertiesMap);
        writeProperties(name, orderedMap);
    }
}
