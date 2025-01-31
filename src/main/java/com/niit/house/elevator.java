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

import java.io.InputStream;
import java.text.DecimalFormat;
import java.util.Scanner;

public class elevator {
    public static void main(String[] args) throws Exception {
        // 创建 Flink 执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从本地文件读取数据
//        String inputPath = "input/ershoufang.csv"; // 替换为你的本地文件路径
//        DataStream<String> inputFile = env.readTextFile(inputPath);

        DataStream<String> fileStream;
        // 从 resources 目录读取文件
        String resourcePath = "input/ershoufang.csv";
        InputStream inputStream = test.class.getClassLoader().getResourceAsStream(resourcePath);
        if (inputStream == null) {
            throw new RuntimeException("资源文件未找到: " + resourcePath);
        }

        Scanner scanner = new Scanner(inputStream, "UTF-8").useDelimiter("\\A");
        String fileContent = scanner.hasNext() ? scanner.next() : "";
        scanner.close();

        // 将文件内容按行拆分
        fileStream = env.fromElements(fileContent.split("\\n"));

        // 解析数据并计算每年平均梯户比
        DataStream<Tuple3<String, Double, Long>> avgElevatorRatio = fileStream
                .filter(line -> !line.startsWith("\"id\"")) // 过滤掉标题行
                .map((MapFunction<String, Tuple2<String, Double>>) line -> {
                    // 假设文件每行格式为 CSV，按逗号分隔，字段在双引号内
                    String[] fields = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"); // 正则处理带引号的逗号分隔

                    String yearBuilt = fields[11].replaceAll("[^\\d]", ""); // 提取年份
                    String elevatorRatio = fields[16].replace("\"", "");  // 提取梯户比字段

                    // 解析梯户比（格式如 "2梯4户"）
                    double ratio = 0.0;
                    if (elevatorRatio.matches("\\d+梯\\d+户")) {
                        String[] parts = elevatorRatio.split("梯|户");
                        int elevators = Integer.parseInt(parts[0].trim());
                        int households = Integer.parseInt(parts[1].trim());
                        ratio = (double) elevators / households; // 计算梯户比
                    }

                    return new Tuple2<>(yearBuilt, ratio);
                })
                .returns(TypeInformation.of(new TypeHint<Tuple2<String, Double>>() {}))
                .keyBy((KeySelector<Tuple2<String, Double>, String>) value -> value.f0) // 按年份分组
                .reduce((t1, t2) -> new Tuple2<>(t1.f0, t1.f1 + t2.f1)) // 累加梯户比
                .map(new MapFunction<Tuple2<String, Double>, Tuple3<String, Double, Long>>() {
                    private final java.util.HashMap<String, Long> countMap = new java.util.HashMap<>();
                    private final DecimalFormat df = new DecimalFormat("#.00");

                    @Override
                    public Tuple3<String, Double, Long> map(Tuple2<String, Double> value) throws Exception {
                        countMap.put(value.f0, countMap.getOrDefault(value.f0, 0L) + 1); // 统计条目数
                        double average = value.f1 / countMap.get(value.f0); // 计算平均值
                        return new Tuple3<>(value.f0, Double.valueOf(df.format(average)), countMap.get(value.f0));
                    }
                })
                .returns(TypeInformation.of(new TypeHint<Tuple3<String, Double, Long>>() {}));

        // 将结果保存到 MySQL
        avgElevatorRatio.addSink(JdbcSink.sink(
                "REPLACE INTO year_avg_elevator_ratio (year_built, avg_elevator_ratio, count) VALUES (?, ?, ?)",
                (ps, t) -> {
                    ps.setString(1, t.f0); // 年份
                    ps.setDouble(2, t.f1); // 平均比值（保留两位小数）
                    ps.setLong(3, t.f2);   // 数据条数
                },
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUrl("jdbc:mysql://192.168.56.104:3306/vexan") // 替换为你的 MySQL URL
                        .withUsername("root") // 替换为用户名
                        .withPassword("root") // 替换为密码
                        .build()
        ));

        // 启动 Flink 作业
        env.execute("Yearly Average Elevator Ratio Analysis with JDBC");
    }
}
