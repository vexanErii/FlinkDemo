package com.niit.getData;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Properties;

public class Tokafka {
    public static void main(String[] args) {
        // Kafka 配置
        Properties props = new Properties();
        props.put("bootstrap.servers", "127.0.0.1:9092"); // Kafka 服务器地址
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 创建 Kafka 生产者
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // 读取本地文件并发送到 Kafka
        String filePath = "input/ershoufang.csv"; // 替换为你的文件路径
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                // 发送数据到 Kafka topic
                producer.send(new ProducerRecord<>("ershoufang-data", null, line));
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // 关闭生产者
            producer.close();
        }

        System.out.println("数据已上传到 Kafka Topic！");
    }
}
