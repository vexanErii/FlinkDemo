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

import java.io.InputStream;
import java.text.DecimalFormat;
import java.util.Scanner;

public class monthlyprice {
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

        // 格式化工具：保留两位小数
        DecimalFormat df = new DecimalFormat("#.00");

        // 解析数据并计算每月的平均 ADR
        DataStream<Tuple2<Integer, Double>> monthlyAverageADR = fileStream
                .filter(line -> !line.startsWith("\"hotel\"")) // 过滤掉标题行
                .map((MapFunction<String, Tuple2<Integer, Double>>) line -> {
                    // 假设每行格式为 CSV，按逗号分隔
                    String cleanedLine = line.replace("\"", "");
                    String[] fields = cleanedLine.split(","); // 按逗号分隔

                    Integer month = Integer.parseInt(fields[4].trim()); // arrival_date_month
                    Double adr = Double.parseDouble(fields[18].trim());   // adr

                    return new Tuple2<>(month, adr);
                })
                .returns(TypeInformation.of(new TypeHint<Tuple2<Integer, Double>>() {}))
                .keyBy(tuple -> tuple.f0) // 按月份分组
                .reduce((t1, t2) -> new Tuple2<>(t1.f0, t1.f1 + t2.f1)) // 累加房价总和
                .map(new MapFunction<Tuple2<Integer, Double>, Tuple2<Integer, Double>>() {
                    private final java.util.HashMap<Integer, Long> countMap = new java.util.HashMap<>();

                    @Override
                    public Tuple2<Integer, Double> map(Tuple2<Integer, Double> value) throws Exception {
                        countMap.put(value.f0, countMap.getOrDefault(value.f0, 0L) + 1);
                        double average = value.f1 / countMap.get(value.f0);
                        // 保留两位小数
                        return new Tuple2<>(value.f0, Double.valueOf(df.format(average)));
                    }
                })
                .returns(TypeInformation.of(new TypeHint<Tuple2<Integer, Double>>() {}));

        // 将统计结果输出到控制台
        monthlyAverageADR.addSink(JdbcSink.sink(
                "REPLACE INTO monthly_avg_adr (month,avg_adr) VALUES (?, ?)",
                (ps,t) -> {
                    ps.setInt(1,t.f0);
                    ps.setDouble(2,t.f1);
                },
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUrl("jdbc:mysql://192.168.56.104:3306/vexan") // 替换为你的 MySQL URL
                        .withUsername("root") // 替换为用户名
                        .withPassword("root") // 替换为密码
                        .build()
        ));

        // 启动 Flink 作业
        env.execute("Flink Monthly Average ADR Calculation");
    }
}
