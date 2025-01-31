package com.niit.house;

import com.niit.test.test;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.InputStream;
import java.util.Properties;
import java.util.Scanner;

public class facefocus {
    public static void main(String[] args) throws Exception {
        // 创建 Flink 执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从本地文件读取数据
//        String inputPath = "input/ershoufang.csv"; // 替换为你的本地文件路径
//        DataStream<String> inputFile = env.readTextFile(inputPath);
        //----------------------资源目录读取数据
//        DataStream<String> fileStream;
//        // 从 resources 目录读取文件
//        String resourcePath = "input/ershoufang.csv";
//        InputStream inputStream = test.class.getClassLoader().getResourceAsStream(resourcePath);
//        if (inputStream == null) {
//            throw new RuntimeException("资源文件未找到: " + resourcePath);
//        }
//
//        Scanner scanner = new Scanner(inputStream, "UTF-8").useDelimiter("\\A");
//        String fileContent = scanner.hasNext() ? scanner.next() : "";
//        scanner.close();
//
//        // 将文件内容按行拆分
//        fileStream = env.fromElements(fileContent.split("\\n"));
        //--------------------------------------
        // Kafka 消费者配置
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        kafkaProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "neighborhood-group");
        kafkaProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaProps.setProperty("auto.offset.reset", "earliest");

        // 创建 Kafka 数据流
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                "ershoufang-data", // Kafka topic
                new org.apache.flink.api.common.serialization.SimpleStringSchema(),
                kafkaProps
        );

        // 从 Kafka 获取数据流
        DataStream<String> kafkaStream = env.addSource(kafkaConsumer);



        // 统计不同朝向的关注人数
        DataStream<Tuple3<String, Integer, Long>> orientationFocusCount = kafkaStream
                .filter(line -> !line.startsWith("\"id\"")) // 过滤掉标题行
                .map((MapFunction<String, Tuple2<String, Integer>>) line -> {
                    // 假设文件每行格式为 CSV，按逗号分隔，字段在双引号内
                    String[] fields = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"); // 正则处理带引号的逗号分隔

                    String orientation = fields[8].replace("\"", "").trim(); // 提取朝向
                    String focusStr = fields[5].replace("\"", "").trim(); // 提取关注人数字段

                    // 清理关注人数字符串，只保留数字部分
                    int focus;
                    try {
                        // 提取关注人数中的数字部分
                        focus = Integer.parseInt(focusStr.replaceAll("[^0-9]", "")); // 只保留数字
                    } catch (NumberFormatException e) {
                        focus = 0; // 如果转换失败，默认为 0
                    }

                    return new Tuple2<>(orientation, focus);
                })
                .returns(TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}))
                .keyBy((KeySelector<Tuple2<String, Integer>, String>) value -> value.f0) // 按朝向分组
                .reduce((t1, t2) -> new Tuple2<>(t1.f0, t1.f1 + t2.f1)) // 累加关注人数
                .map(new MapFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Long>>() {
                    private final java.util.HashMap<String, Long> countMap = new java.util.HashMap<>();

                    @Override
                    public Tuple3<String, Integer, Long> map(Tuple2<String, Integer> value) throws Exception {
                        countMap.put(value.f0, countMap.getOrDefault(value.f0, 0L) + 1); // 统计条目数
                        return new Tuple3<>(value.f0, value.f1, countMap.get(value.f0)); // 返回朝向、总关注人数和条目数
                    }
                })
                .returns(TypeInformation.of(new TypeHint<Tuple3<String, Integer, Long>>() {}));

        // 将结果保存到 MySQL
        orientationFocusCount.addSink(JdbcSink.sink(
                "REPLACE INTO orientation_focus_count (orientation, total_focus, count) VALUES (?, ?, ?)",
                (ps, t) -> {
                    ps.setString(1, t.f0); // 朝向
                    ps.setInt(2, t.f1); // 总关注人数
                    ps.setLong(3, t.f2); // 数据条数
                },
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUrl("jdbc:mysql://192.168.56.104:3306/vexan") // 替换为你的 MySQL URL
                        .withUsername("root") // 替换为用户名
                        .withPassword("root") // 替换为密码
                        .build()
        ));

        // 启动 Flink 作业
        env.execute("Orientation Focus Count Analysis with JDBC");
    }
}
