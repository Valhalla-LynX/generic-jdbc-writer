package com.valhalla.holder.mqtt;

import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.junit.Test;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * @author : LynX
 * @description:
 * @create: 2021-10-26 09:22
 **/
@SpringBootTest
@Slf4j
public class MqttTest {
    @Test
    public void send() throws MqttException {
        MqttClient mqttClient = new MqttClient("tcp://localhost:1883", "test" + "-" + System.currentTimeMillis() % 1000, new MemoryPersistence());
        mqttClient.connect();
        String msg = "{\n" +
                "  \"type\": \"single\",\n" +
                "  \"table\": \"test\",\n" +
                "  \"data\": [0,\"test\"]\n" +
                "}";
        for (int i = 0; i < 100_000; i++) {
            mqttClient.publish("test", msg.getBytes(), 0, false);
        }
        System.out.println("done");
    }

}
