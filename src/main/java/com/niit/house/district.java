package com.niit.house;

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
import java.util.Scanner;

public class district { // 每个行政区房源总数
    public static void main(String[] args) throws Exception {
        // 创建 Flink 执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

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

            // 跳过第一行（标题行）并解析数据，每个行政区房源总数
        DataStream<Tuple2<String, Integer>> districtCounts = fileStream
                .filter(line -> !line.startsWith("\"id\"")) // 过滤掉标题行
                .map((MapFunction<String, Tuple2<String, Integer>>) line -> {
                    // 假设文件每行格式为 CSV，按逗号分隔，字段在双引号内
                    String[] fields = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"); // 正则处理带引号的逗号分隔
                    String district = fields[13].replace("\"", ""); // 第 14 列为行政区字段
                    return new Tuple2<>(district, 1);
                })
                .returns(TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}))
                .keyBy(tuple -> tuple.f0) // 按 district 分组
                .sum(1); // 累加房源数量

        // 将统计结果写入 MySQL
        districtCounts.addSink(JdbcSink.sink(
                "REPLACE INTO district_stats (district, house_count) VALUES (?, ?)", // MySQL 插入语句
                (ps, t) -> {
                    ps.setString(1, t.f0); // 设置 district 字段
                    ps.setInt(2, t.f1);    // 设置房源总数
                },
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUrl("jdbc:mysql://192.168.56.104:3306/vexan") // 替换为你的 MySQL URL
                        .withUsername("root") // 替换为用户名
                        .withPassword("root") // 替换为密码
                        .build()
        ));

        // 启动 Flink 作业
        env.execute("Flink Local File District Analysis");
    }
}
