package com.niit.test;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcInputFormat;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.api.java.typeutils.RowTypeInfo;

import java.text.DecimalFormat;
import java.util.Date;

public class jdbc {
    public static void main(String[] args) throws Exception {
        // 创建 Flink 执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 定义 RowTypeInfo，指定每一列的类型
        RowTypeInfo rowTypeInfo = new RowTypeInfo(
                TypeInformation.of(String.class), // title
                TypeInformation.of(String.class), // url
                TypeInformation.of(String.class), // price
                TypeInformation.of(String.class), // unit_price
                TypeInformation.of(Integer.class), // focus
                TypeInformation.of(String.class), // layout
                TypeInformation.of(String.class), // floor
                TypeInformation.of(String.class), // orientation
                TypeInformation.of(String.class), // decoration
                TypeInformation.of(String.class), // area
                TypeInformation.of(String.class), // year_built
                TypeInformation.of(String.class), // community
                TypeInformation.of(String.class), // district
                TypeInformation.of(String.class), // neighborhood
                TypeInformation.of(String.class), // elevator
                TypeInformation.of(String.class), // elevator_ratio
                TypeInformation.of(Date.class), // post_date
                TypeInformation.of(Date.class)  // update_date
        );

        // 配置 JDBC 输入
        JdbcInputFormat jdbcInputFormat = JdbcInputFormat.buildJdbcInputFormat()
                .setDrivername("com.mysql.cj.jdbc.Driver")
                .setDBUrl("jdbc:mysql://localhost:3306/vexan") // 替换为你的数据库 URL
                .setUsername("root") // 替换为你的数据库用户名
                .setPassword("root") // 替换为你的数据库密码
                .setQuery("SELECT title, url, price, unit_price, focus, layout, floor, orientation, decoration, area, year_built, community, district, neighborhood, elevator, elevator_ratio, post_date, update_date FROM ershoufang") // 替换为你的表名和字段
                .setRowTypeInfo(rowTypeInfo)
                .finish();

        // 从 JDBC 读取数据
        DataStream<Row> jdbcStream = env.createInput(jdbcInputFormat);

        // 解析数据并计算每个街区的平均房价
        DataStream<Tuple3<String, Double, Long>> neighborhoodAveragePrice = jdbcStream
                .map((MapFunction<Row, Tuple2<String, Double>>) row -> {
                    // 从 Row 中提取街区字段和单价字段
                    String neighborhood = row.getField(13).toString(); // 假设 "neighborhood" 字段在第 13 列（从 0 开始计数）
                    String unitPriceStr = row.getField(3).toString();  // 假设 "unit_price" 字段在第 3 列
                    Double unitPrice = Double.parseDouble(unitPriceStr.replace(",", "").replace("元/平", ""));
                    return new Tuple2<>(neighborhood, unitPrice);
                })
                .returns(TypeInformation.of(new TypeHint<Tuple2<String, Double>>() {}))
                .keyBy(tuple -> tuple.f0) // 按街区分组
                .reduce((t1, t2) -> new Tuple2<>(t1.f0, t1.f1 + t2.f1)) // 累加房价总和
                .map(new MapFunction<Tuple2<String, Double>, Tuple3<String, Double, Long>>() {
                    private final java.util.HashMap<String, Long> countMap = new java.util.HashMap<>();
                    private final DecimalFormat df = new DecimalFormat("#.00"); // 格式化两位小数

                    @Override
                    public Tuple3<String, Double, Long> map(Tuple2<String, Double> value) throws Exception {
                        countMap.put(value.f0, countMap.getOrDefault(value.f0, 0L) + 1);
                        Double average = value.f1 / countMap.get(value.f0);
                        // 保留两位小数
                        return new Tuple3<>(value.f0, Double.valueOf(df.format(average)), countMap.get(value.f0));
                    }
                })
                .returns(TypeInformation.of(new TypeHint<Tuple3<String, Double, Long>>() {}));

        // 将统计结果写入 MySQL
        neighborhoodAveragePrice.addSink(JdbcSink.sink(
                "REPLACE INTO neighborhood_avg_price (neighborhood, avg_price, count) VALUES (?, ?, ?)", // MySQL 插入语句
                (ps, t) -> {
                    ps.setString(1, t.f0); // 设置街区名称
                    ps.setDouble(2, t.f1); // 设置平均房价（保留两位小数）
                    ps.setLong(3, t.f2);   // 设置房源数量
                },
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUrl("jdbc:mysql://localhost:3306/vexan") // 替换为你的 MySQL URL
                        .withUsername("root") // 替换为用户名
                        .withPassword("root") // 替换为密码
                        .build()
        ));

        // 启动 Flink 作业
        env.execute("Flink JDBC Neighborhood Average Price Analysis");
    }
}
