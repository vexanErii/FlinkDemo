package com.niit.getData;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Random;
import java.util.Date;

public class DataGenerator {
    private static final String URL = "jdbc:mysql://localhost:3306/vexan";
    private static final String USER = "root";
    private static final String PASSWORD = "root";
    private static final int BATCH_SIZE = 1000;  // 每次批量插入的记录数
    private static final int TARGET_SIZE_MB = 100; // 目标数据大小：100MB

    public static void main(String[] args) {
        try {
            // 连接数据库
            Connection conn = DriverManager.getConnection(URL, USER, PASSWORD);
            conn.setAutoCommit(false);  // 关闭自动提交，提升插入效率

            // 获取随机生成器
            Random random = new Random();

            // 计算需要生成多少条数据
            int targetRecords = calculateTargetRecords();
            int generatedRecords = 0;

            // 循环生成数据并插入
            while (generatedRecords < targetRecords) {
                PreparedStatement stmt = conn.prepareStatement(
                        "INSERT INTO ershoufang (title, url, price, unit_price, focus, layout, floor, orientation, decoration, area, year_built, community, district, neighborhood, elevator, elevator_ratio, post_date, update_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                );

                for (int i = 0; i < BATCH_SIZE && generatedRecords < targetRecords; i++) {
                    // 模拟房源数据
                    String title = generateTitle(random);
                    String url = generateUrl();
                    String price = generatePrice(random);
                    String unitPrice = generateUnitPrice(random);
                    int focus = random.nextInt(1000);
                    String layout = generateLayout(random);
                    String floor = generateFloor(random);
                    String orientation = generateOrientation(random);
                    String decoration = generateDecoration(random);
                    String area = generateArea(random);
                    String yearBuilt = generateYearBuilt(random);
                    String community = generateCommunity(random);
                    String district = generateDistrict(random);
                    String neighborhood = generateNeighborhood(random);
                    String elevator = generateElevator(random);
                    String elevatorRatio = generateElevatorRatio(random);
                    String postDate = generateDate();
                    String updateDate = generateDate();

                    // 填充PreparedStatement
                    stmt.setString(1, title);
                    stmt.setString(2, url);
                    stmt.setString(3, price);
                    stmt.setString(4, unitPrice);
                    stmt.setInt(5, focus);
                    stmt.setString(6, layout);
                    stmt.setString(7, floor);
                    stmt.setString(8, orientation);
                    stmt.setString(9, decoration);
                    stmt.setString(10, area);
                    stmt.setString(11, yearBuilt);
                    stmt.setString(12, community);
                    stmt.setString(13, district);
                    stmt.setString(14, neighborhood);
                    stmt.setString(15, elevator);
                    stmt.setString(16, elevatorRatio);
                    stmt.setString(17, postDate);
                    stmt.setString(18, updateDate);

                    // 执行插入操作
                    stmt.addBatch();
                    generatedRecords++;

                    if (generatedRecords % BATCH_SIZE == 0) {
                        stmt.executeBatch();  // 执行批量插入
                        conn.commit();
                        System.out.println("已生成 " + generatedRecords + " 条数据");
                    }
                }

                stmt.executeBatch();  // 执行剩余的插入
                conn.commit();
            }

            conn.close();
            System.out.println("数据生成完毕，总共插入 " + generatedRecords + " 条数据，达到了 100MB");

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static int calculateTargetRecords() {
        // 假设每条数据平均大小约为2KB，100MB = 100 * 1024 * 1024 字节
        return (200 * 1024 * 1024) / (2 * 1024);  // 返回需要生成的记录数
    }

    private static String generateTitle(Random random) {
        String[] titles = {
                "高层朝南2房，拎包入住，采光充足，进岛首站",
                "精装两室一厅，采光良好，地段优越",
                "南北通透三室两厅，拎包入住",
                "两房一厅，经典户型，环境优美",
                "小区绿化好，环境优美，物业管理也很规范",
                "自住装修，采光通风好，视野好，进岛方便，高楼层看海",
                "此房朝南通透格局，楼层好视野宽阔，居住环境舒适",
                "高层边套，景观视野良好，格局方正，采光好"
        };
        return titles[random.nextInt(titles.length)];
    }

    private static String generateUrl() {
        return "https://xm.lianjia.com/ershoufang/" + (1000000000 + new Random().nextInt(1000000));
    }

    private static String generatePrice(Random random) {
        return (random.nextInt(50) + 50) + "万元";
    }

    private static String generateUnitPrice(Random random) {
        return random.nextInt(30000) + "元/平";
    }

    private static String generateLayout(Random random) {
        return (random.nextInt(5) + 1) + "室" + (random.nextInt(3) + 1) + "厅";
    }

    private static String generateFloor(Random random) {
        int floorLevel = random.nextInt(3);  // 生成0, 1, 2，分别代表低、中、高楼层
        switch (floorLevel) {
            case 0:
                return "低楼层/共" + (random.nextInt(20) + 10) + "层";
            case 1:
                return "中楼层/共" + (random.nextInt(20) + 10) + "层";
            case 2:
                return "高楼层/共" + (random.nextInt(20) + 10) + "层";
            default:
                return "未知楼层";  // 额外的默认情况
        }
    }


    private static String generateOrientation(Random random) {
        String[] orientations = {"南", "北", "东", "西", "东南", "西南", "东北", "西北"};
        return orientations[random.nextInt(orientations.length)];
    }

    private static String generateDecoration(Random random) {
        String[] decorations = {"精装", "简装", "毛坯"};
        return decorations[random.nextInt(decorations.length)];
    }

    private static String generateArea(Random random) {
        return (random.nextInt(80) + 50) + "平米";
    }

    private static String generateYearBuilt(Random random) {
        return "20" + (random.nextInt(10) + 10) + "年建";
    }

    private static String generateCommunity(Random random) {
        String[] community = {"东坑安居房", "禹洲卢卡小镇", "东坑安居房","东坑安居房","滨水二里", "黄金大厦","禹洲卢卡小镇", "禹洲大学城","禹洲大学城", "桃源大厦", "上东美地", "恒丰花园","恒丰花园","恒丰花园", "海晟维多利亚", "厦门中央公园", "莲花尚院"};
        return community[random.nextInt(community.length)];
    }

    private static String generateDistrict(Random random) {
        String[] district = {"海沧","海沧","海沧", "翔安", "集美", "集美","湖里", "同安", "同安","思明"};
        return district[random.nextInt(district.length)];
    }

    private static String generateNeighborhood(Random random) {
        String[] neighborhood = {"滨海社区", "滨海社区","滨海社区","新店", "集美新城", "杏林桥头","杏林桥头", "翔安新城", "邮轮城","邮轮城","江头","瑞景","南山路"};
        return neighborhood[random.nextInt(neighborhood.length)];
    }

    private static String generateElevator(Random random) {
        return random.nextBoolean() ? "有" : "无";
    }

    private static String generateElevatorRatio(Random random) {
        return (random.nextInt(2) + 1) + "梯" + (random.nextInt(6) + 1) + "户";
    }

    private static String generateDate() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        return sdf.format(new Date());
    }
}
