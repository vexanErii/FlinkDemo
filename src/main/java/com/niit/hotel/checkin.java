package com.niit.hotel;

import com.niit.test.test;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;

import java.io.InputStream;
import java.util.Scanner;

public class checkin {
    public static void main(String[] args) throws Exception {
        // 创建 Flink 执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        String inputPath = "input/hotel_bookings.csv"; // 替换为你的本地文件路径
//        DataStream<String> fileStream = env.readTextFile(inputPath);

        DataStream<String> fileStream;
        // 从 resources 目录读取文件
        String resourcePath = "input/hotel_booking.csv";
        InputStream inputStream = checkin.class.getClassLoader().getResourceAsStream(resourcePath);
        if (inputStream == null) {
            throw new RuntimeException("资源文件未找到: " + resourcePath);
        }

        Scanner scanner = new Scanner(inputStream, "UTF-8").useDelimiter("\\A");
        String fileContent = scanner.hasNext() ? scanner.next() : "";
        scanner.close();

        // 将文件内容按行拆分
        fileStream = env.fromElements(fileContent.split("\\n"));

        // 解析数据并计算每个月不同类型酒店的实际入住量
        DataStream<Tuple3<String, Integer, Long>> monthlyBookings = fileStream
                .filter(line -> !line.startsWith("\"hotel\"")) // 过滤掉标题行
                .map((MapFunction<String, Tuple3<String, Integer, Long>>) line -> {
                    String cleanedLine = line.replace("\"", ""); // 去除引号
                    String[] fields = cleanedLine.split(","); // 按逗号分隔

                    String hotelType = fields[0].trim(); // 酒店类型（索引为0）
                    Integer month = Integer.valueOf(fields[4]); // arrival_date_month（索引为4）
                    Integer isCanceled = Integer.valueOf(fields[1]); // is_canceled（索引为1）

                    // 若未取消（is_canceled = 0），则计为一次入住
                    long count = isCanceled == 0 ? 1 : 0;
                    return new Tuple3<>(hotelType, month, count);
                })
                .returns(TypeInformation.of(new TypeHint<Tuple3<String, Integer, Long>>() {}))
                .keyBy(new KeySelector<Tuple3<String, Integer, Long>, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> getKey(Tuple3<String, Integer, Long> value) throws Exception {
                        return Tuple2.of(value.f0, value.f1); // 返回酒店类型和月份作为键
                    }
                }) // 按酒店类型和月份分组
                .sum(2) // 聚合每种类型酒店每个月的入住量
                .returns(TypeInformation.of(new TypeHint<Tuple3<String, Integer, Long>>() {}));

        // 将统计结果写入 MySQL
        monthlyBookings.addSink(JdbcSink.sink(
                "REPLACE INTO monthly_booking_counts (month, hotel_type, booking_count) VALUES (?, ?, ?)", // MySQL 插入语句
                (ps, t) -> {
                    ps.setInt(1, t.f1); // 设置酒店类型
                    ps.setString(2, t.f0); // 设置月份
                    ps.setLong(3, t.f2); // 设置入住数量
                },
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUrl("jdbc:mysql://192.168.56.104:3306/vexan") // 替换为你的 MySQL URL
                        .withUsername("root") // 替换为用户名
                        .withPassword("root") // 替换为密码
                        .build()
        ));

        // 启动 Flink 作业
        env.execute("Flink Monthly Hotel Booking Analysis");
    }
}
