package com.niit.test;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.text.DecimalFormat;
import java.util.Properties;

public class test1 {
    public static void main(String[] args) throws Exception {
        // 创建 Flink 执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Kafka 消费者配置
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        kafkaProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "neighborhood-group");
        kafkaProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaProps.setProperty("auto.offset.reset", "earliest");

        // 创建 Kafka 数据流
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                "ershoufang", // Kafka topic
                new org.apache.flink.api.common.serialization.SimpleStringSchema(),
                kafkaProps
        );

        // 从 Kafka 获取数据流
        DataStream<String> kafkaStream = env.addSource(kafkaConsumer);

        // 数据格式化工具
        DecimalFormat df = new DecimalFormat("#.00");

        // 解析数据并计算每个街区的平均房价
        DataStream<Tuple3<String, Double, Long>> neighborhoodAveragePrice = kafkaStream
                .filter(line -> !line.startsWith("\"id\"")) // 过滤掉标题行
                .map((MapFunction<String, Tuple2<String, Double>>) line -> {
                    // 假设文件每行格式为 CSV，按逗号分隔，字段在双引号内
                    String[] fields = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"); // 处理带引号的逗号分隔
                    String neighborhood = fields[14].replace("\"", ""); // 第 15 列为街区名称
                    String unitPriceStr = fields[4].replace("\"", "").replace("元/平", ""); // 第 5 列为单价
                    Double unitPrice = Double.parseDouble(unitPriceStr);
                    return new Tuple2<>(neighborhood, unitPrice);
                })
                .returns(TypeInformation.of(new TypeHint<Tuple2<String, Double>>() {}))
                .keyBy(tuple -> tuple.f0) // 按街区分组
                .reduce((t1, t2) -> new Tuple2<>(t1.f0, t1.f1 + t2.f1)) // 累加房价总和
                .map(new MapFunction<Tuple2<String, Double>, Tuple3<String, Double, Long>>() {
                    private final java.util.HashMap<String, Long> countMap = new java.util.HashMap<>();

                    @Override
                    public Tuple3<String, Double, Long> map(Tuple2<String, Double> value) throws Exception {
                        countMap.put(value.f0, countMap.getOrDefault(value.f0, 0L) + 1);
                        Double average = value.f1 / countMap.get(value.f0);
                        return new Tuple3<>(value.f0, Double.valueOf(df.format(average)), countMap.get(value.f0));
                    }
                })
                .returns(TypeInformation.of(new TypeHint<Tuple3<String, Double, Long>>() {}));

        // 将统计结果写入 MySQL
        neighborhoodAveragePrice.addSink(JdbcSink.sink(
                "REPLACE INTO test (neighborhood, avg_price, count) VALUES (?, ?, ?)", // MySQL 插入语句
                (ps, t) -> {
                    ps.setString(1, t.f0); // 设置街区名称
                    ps.setDouble(2, t.f1); // 设置平均房价（保留两位小数）
                    ps.setLong(3, t.f2);   // 设置房源数量
                },
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUrl("jdbc:mysql://192.168.56.104:3306/vexan") // 替换为你的 MySQL URL
                        .withUsername("root") // 替换为用户名
                        .withPassword("root") // 替换为密码
                        .build()
        ));

        // 启动 Flink 作业
        env.execute("Kafka Neighborhood Average Price Analysis");
    }
}
