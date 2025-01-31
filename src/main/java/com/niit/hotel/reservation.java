package com.niit.hotel;

import com.niit.test.test;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.InputStream;
import java.util.Scanner;

public class reservation {
    public static void main(String[] args) throws Exception {
        // 创建 Flink 执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从本地文件读取数据
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
        // 解析数据并计算实际入住量
        DataStream<Tuple4<String, Long, Long, Long>> statistics = fileStream
                .filter(line -> !line.startsWith("\"hotel\"")) // 过滤掉标题行
                .map(new MapFunction<String, Tuple4<String, Long, Long, Long>>() {
                    @Override
                    public Tuple4<String, Long, Long, Long> map(String line) throws Exception {
                        // 清洗数据
                        String cleanedLine = line.replace("\"", "");
                        String[] fields = cleanedLine.split(","); // 按逗号分隔

                        String hotelType = fields[0].trim(); // 酒店类型
                        Integer isCanceled = Integer.parseInt(fields[1].trim()); // 是否取消
                        Integer staysInNights = Integer.parseInt(fields[8].trim()); // 实际入住天数

                        // 统计预定数、取消数和实际入住数
                        long bookingCount = 1; // 每一行代表一个预定
                        long canceledCount = isCanceled == 1 ? 1 : 0; // 如果取消，则计数为1，否则为0
                        long actualStayCount = staysInNights; // 实际入住天数

                        return new Tuple4<>(hotelType, bookingCount, canceledCount, actualStayCount);
                    }
                })
                .returns(TypeInformation.of(new TypeHint<Tuple4<String, Long, Long, Long>>() {}))
                .keyBy(tuple -> tuple.f0) // 按酒店类型分组
                .reduce(new ReduceFunction<Tuple4<String, Long, Long, Long>>() {
                    @Override
                    public Tuple4<String, Long, Long, Long> reduce(Tuple4<String, Long, Long, Long> t1, Tuple4<String, Long, Long, Long> t2) throws Exception {
                        return new Tuple4<>(
                                t1.f0, // 酒店类型
                                t1.f1 + t2.f1, // 预定数
                                t1.f2 + t2.f2, // 取消数
                                t1.f3 + t2.f3  // 实际入住数
                        );
                    }
                });

        // 将统计结果输出到 MySQL 数据库
        statistics.addSink(JdbcSink.sink(
                "REPLACE INTO hotel_statistics (hotel_type, booking_count, canceled_count, actual_stay_count) VALUES (?, ?, ?, ?)",
                (ps, t) -> {
                    ps.setString(1, t.f0); // 酒店类型
                    ps.setLong(2, t.f1);    // 预定数
                    ps.setLong(3, t.f2);    // 取消数
                    ps.setLong(4, t.f3);    // 实际入住数
                },
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUrl("jdbc:mysql://192.168.56.104:3306/vexan") // 替换为你的 MySQL URL
                        .withUsername("root") // 替换为用户名
                        .withPassword("root") // 替换为密码
                        .build()
        ));

        // 启动 Flink 作业
        env.execute("Hotel Reservation Analysis");
    }
}
