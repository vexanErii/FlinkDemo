package com.niit.house;

import com.niit.test.test;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.InputStream;
import java.util.Scanner;

public class tagline {
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

        // 解析数据并统计词频
        DataStream<Tuple2<String, Long>> wordCounts = fileStream
                .filter(line -> !line.startsWith("\"id\"")) // 过滤掉标题行
                .flatMap((FlatMapFunction<String, Tuple2<String, Long>>) (line, out) -> {
                    // 按逗号分隔，提取 title 字段
                    String[] fields = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"); // 正则处理带引号的逗号分隔
                    if (fields.length > 1) { // 确保字段索引有效
                        String promotionText = fields[1].replace("\"", "").trim(); // 提取标题

                        // 将宣传语按逗号和空格拆分成单词
                        String[] words = promotionText.split("[，\\s]+");
                        for (String word : words) {
                            if (!word.trim().isEmpty()) {
                                out.collect(new Tuple2<>(word, 1L)); // 输出每个单词及其计数1
                            }
                        }
                    }
                })
                .returns(TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {}))
                .keyBy(tuple -> tuple.f0) // 按单词分组
                .sum(1); // 汇总计数

        // 将结果保存到 MySQL
        wordCounts.addSink(JdbcSink.sink(
                "REPLACE INTO word_count (word, count) VALUES (?, ?)",
                (ps, t) -> {
                    ps.setString(1, t.f0); // 单词
                    ps.setLong(2, t.f1);   // 计数
                },
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUrl("jdbc:mysql://192.168.56.104:3306/vexan") // 替换为你的 MySQL URL
                        .withUsername("root") // 替换为用户名
                        .withPassword("root") // 替换为密码
                        .build()
        ));

        // 启动 Flink 作业
        env.execute("House Promotion Word Count Analysis with Kafka Input");
    }
}
