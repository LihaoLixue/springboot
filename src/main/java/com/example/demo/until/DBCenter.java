package com.example.demo.until;

import com.example.demo.until.scheduler.ExecuteSQLUtil;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * @author LH
 * @description:
 * @date 2021-09-06 13:52
 */
@Component
public class DBCenter {
    Logger logger = Logger.getLogger(ExecuteSQLUtil.class);
//    @Value("${spring.datasource.url}")
    private static String DB_URL="jdbc:mysql://192.168.31.162:3306/socks?useSSL=false";
//    @Value("${spring.datasource.username}")
    private static String DB_USERNAME="root";
//    @Value("${spring.datasource.password}")
    private static String DB_PWD="123456";

    public static Connection getConnection() {
        Connection connection = null;
        try {
            String driverClassName = "com.mysql.cj.jdbc.Driver";
            Class.forName(driverClassName);
            connection = DriverManager.getConnection(DB_URL, DB_USERNAME, DB_PWD);
//            connection = DriverManager.getConnection(DB_URL, DB_USERNAME, DB_PWD);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return connection;
    }
}
