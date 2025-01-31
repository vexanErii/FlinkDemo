package com.niit.hotel;

import com.niit.test.test;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;

import java.io.InputStream;
import java.util.Scanner;

public class tourists {
    public static void main(String[] args) throws Exception {
        // 创建 Flink 执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        String inputPath = "input/hotel_bookings.csv"; // 替换为你的本地文件路径
//        DataStream<String> fileStream = env.readTextFile(inputPath);

        DataStream<String> fileStream;
        // 从 resources 目录读取文件
        String resourcePath = "input/hotel_booking.csv";
        InputStream inputStream = test.class.getClassLoader().getResourceAsStream(resourcePath);
        if (inputStream == null) {
            throw new RuntimeException("资源文件未找到: " + resourcePath);
        }

        Scanner scanner = new Scanner(inputStream, "UTF-8").useDelimiter("\\A");
        String fileContent = scanner.hasNext() ? scanner.next() : "";
        scanner.close();

        // 将文件内容按行拆分
        fileStream = env.fromElements(fileContent.split("\\n"));

        // 解析数据并统计游客国家
        DataStream<Tuple2<String, Long>> countryStats = fileStream
                .filter(line -> !line.startsWith("\"hotel\"")) // 过滤掉标题行
                .map((MapFunction<String, Tuple2<String, Long>>) line -> {
                    String cleanedLine = line.replace("\"", ""); // 去除引号
                    String[] fields = cleanedLine.split(","); // 按逗号分隔

                    String country = fields[9].trim(); // 国家（索引为9）
                    Long visitorCount = 1L; // 每个未取消的预定算作一名游客

                    return new Tuple2<>(country, visitorCount);
                })
                .returns(TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {}))
                .keyBy(new KeySelector<Tuple2<String, Long>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Long> value) throws Exception {
                        return value.f0; // 返回国家作为键
                    }
                }) // 按国家分组
                .sum(1) // 聚合游客人数
                .returns(TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {}));

        // 将统计结果写入 MySQL
        countryStats.addSink(JdbcSink.sink(
                "REPLACE INTO country_statistics (country, visitor_count) VALUES (?, ?)", // MySQL 插入语句
                (ps, t) -> {
                    ps.setString(1, t.f0); // 设置国家
                    ps.setLong(2, t.f1); // 设置游客人数
                },
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUrl("jdbc:mysql://192.168.56.104:3306/vexan") // 替换为你的 MySQL URL
                        .withUsername("root") // 替换为用户名
                        .withPassword("root") // 替换为密码
                        .build()
        ));

        // 启动 Flink 作业
        env.execute("Flink Hotel Visitor Country Analysis");
    }
}
