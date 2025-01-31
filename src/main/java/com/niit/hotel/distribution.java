package com.niit.hotel;

import com.niit.test.test;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;

import java.io.InputStream;
import java.util.Scanner;

public class distribution {
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

        // 解析数据并统计酒店类型和预定方式
        DataStream<Tuple3<String, String, Long>> bookingMethodStats = fileStream
                .filter(line -> !line.startsWith("\"hotel\"")) // 过滤掉标题行
                .map((MapFunction<String, Tuple3<String, String, Long>>) line -> {
                    String cleanedLine = line.replace("\"", ""); // 去除引号
                    String[] fields = cleanedLine.split(","); // 按逗号分隔

                    String hotelType = fields[0].trim(); // 酒店类型（索引为0）
                    String bookingMethod = fields[10].trim(); // 预定方式（索引为10）
                    Long count = 1L; // 每个预定算作一次

                    return new Tuple3<>(hotelType, bookingMethod, count);
                })
                .returns(TypeInformation.of(new TypeHint<Tuple3<String, String, Long>>() {}))
                .keyBy(new KeySelector<Tuple3<String, String, Long>, String>() {
                    @Override
                    public String getKey(Tuple3<String, String, Long> value) throws Exception {
                        return value.f0 + "-" + value.f1; // 使用酒店类型和预定方式的组合作为键
                    }
                }) // 按酒店类型和预定方式分组
                .sum(2) // 聚合预定数量
                .returns(TypeInformation.of(new TypeHint<Tuple3<String, String, Long>>() {}));

        // 将统计结果写入 MySQL
        bookingMethodStats.addSink(JdbcSink.sink(
                "REPLACE INTO booking_method_statistics (hotel_type, booking_method, booking_count) VALUES (?, ?, ?)", // MySQL 插入语句
                (ps, t) -> {
                    ps.setString(1, t.f0); // 设置酒店类型
                    ps.setString(2, t.f1); // 设置预定方式
                    ps.setLong(3, t.f2); // 设置预定数量
                },
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUrl("jdbc:mysql://192.168.56.104:3306/vexan") // 替换为你的 MySQL URL
                        .withUsername("root") // 替换为用户名
                        .withPassword("root") // 替换为密码
                        .build()
        ));

        // 启动 Flink 作业
        env.execute("Flink Hotel Booking Method Analysis");
    }
}
