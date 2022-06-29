package com.example.demo.until.scheduler;

/**
 * @author LH
 * @description:
 * @date 2021-06-29 14:24
 */

import com.example.demo.until.mysql.MySQLUtil;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.jdbc.ScriptRunner;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.charset.Charset;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.example.demo.until.ddmessage.AssignMessage.assignXX_only_dd;
import static com.example.demo.until.ddmessage.AssignMessage.getAccessToken;
import static com.example.demo.until.scheduler.UuidUtil.alertHtdqMessage;

/**
 * @Author: JCccc
 * @Description:
 * @Date: 2020/11/16
 */
@Component
public class ExecuteSQLUtil {
    Logger logger = Logger.getLogger(ExecuteSQLUtil.class);
    @Value("${spring.datasource.url}")
    private String DB_URL;
    @Value("${spring.datasource.username}")
    private String DB_USERNAME;
    @Value("${spring.datasource.password}")
    private String DB_PWD;
    MySQLUtil mySQLUtil = new MySQLUtil();
    //    ComboPooledDataSource dataSource = MySQLUtil.getConnection_1();
    private static Connection connection;

    public void executeSql(String sqlFileName, Connection connection) throws ClassNotFoundException {
        logger.debug("------------------进入非短信执行逻辑------------------: " + sqlFileName);
        try {
            ScriptRunner runner = new ScriptRunner(connection);
            logger.debug("runner is ----------" + runner);
            Resources.setCharset(Charset.forName("UTF-8")); //设置字符集,不然中文乱码插入错误
            runner.setLogWriter(null);//设置是否输出日志
            Reader read = new FileReader(new File(sqlFileName));
            runner.runScript(read);
//            runner.closeConnection();
//            connection.close();
            read.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    public void executeSql_1(String sqlFileName, String dx_if, String swithc_dx) {
        Connection connection = null;
        ResultSet resultSet = null;
        try {
            connection = mySQLUtil.getConnection();
            if ("1".equals(dx_if)) {
                resultSet = execute(sqlFileName, connection, swithc_dx);
            } else if ("0".equals(dx_if)) {
                executeSql(sqlFileName, connection);
            }
            logger.debug("sql脚本执行完毕");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
//            try {
//                if (!resultSet.isClosed()) {
//                    resultSet.close();
//                }
//                if (!connection.isClosed()) {
//                    connection.close();
//                }
//            } catch (SQLException e) {
//                e.printStackTrace();
//            }

        }
//        ClassPathResource rc = new ClassPathResource(sqlFileName);
//        EncodedResource er = new EncodedResource(rc, "utf-8");
//        ScriptUtils.executeSqlScript(connection, er);
//        return connection;
    }

    /**
     * 读取 SQL 文件，获取 SQL 语句
     *
     * @param sqlFile SQL 脚本文件
     * @return List<sql> 返回所有 SQL 语句的 List
     * @throws Exception
     */
    private static ConcurrentLinkedQueue<String> loadSql(String sqlFile) throws Exception {
        ConcurrentLinkedQueue<String> queue = new ConcurrentLinkedQueue<>();
        try {
            InputStream sqlFileIn = new FileInputStream(sqlFile);
            StringBuffer sqlSb = new StringBuffer();
            byte[] buff = new byte[1024];
            int byteRead = 0;
            while ((byteRead = sqlFileIn.read(buff)) != -1) {
                sqlSb.append(new String(buff, 0, byteRead));
            }
            // Windows 下换行是 /r/n, Linux 下是 /n
            String[] sqlArr = sqlSb.toString().split("(;\\s*\\r\\n)|(;\\s*\\n)");
            for (int i = 0; i < sqlArr.length; i++) {
                String sql = sqlArr[i].replaceAll("--.*", "").trim();
                if (!sql.equals("")) {
                    queue.add(sql);
                }
            }
            sqlFileIn.close();
            return queue;
        } catch (Exception ex) {
            throw new Exception(ex.getMessage());
        }
    }

    /**
     * 自建连接，独立事物中执行 SQL 文件
     *
     * @param sqlFile SQL 脚本文件
     * @throws Exception
     */
    public ResultSet execute(String sqlFile, Connection conn, String switch_dx) throws Exception {
        logger.debug("-----------进入短信调度区域----------------");
        ResultSet resultSet = null;
        Statement stmt = null;
        long begin_time = System.currentTimeMillis();
        try {
            ConcurrentLinkedQueue<String> sqlList = loadSql(sqlFile);
            conn.setAutoCommit(true);
            stmt = conn.createStatement();
            int i = 1;
            String max_sj = null;
            while (!sqlList.isEmpty()) {
                String sql = sqlList.poll();
                if (i == 1 && (sql.startsWith("SELECT") || sql.startsWith("select"))) {
                    resultSet = stmt.executeQuery(sql);
                    while (resultSet.next()) {
                        max_sj = resultSet.getString("max_sj");
                        logger.info("告警入参时间: " + max_sj);
                    }
                    i++;
                } else if (i == 2 && (sql.startsWith("insert") || sql.startsWith("INSERT"))) {
                    stmt.execute(sql.replaceAll("max_sj", "'" + max_sj + "'"));
                    i++;
                } else if (i == 3 && (sql.startsWith("SELECT") || sql.startsWith("select"))) {
                    resultSet = stmt.executeQuery(sql.replaceAll("max_sj", "'" + max_sj + "'"));
                    if ("1".equals(switch_dx)) {
                        String accessToken = getAccessToken();
                        while (resultSet.next()) {
                            String bz = resultSet.getString("bz");
                            if ("1".equals(bz)) {
                                logger.debug("进入发送短信的逻辑");
                                String yybmc = resultSet.getString("yybmc");
                                String khh = resultSet.getString("khh");
                                String khxm = resultSet.getString("khxm");
                                String dqlx1cnt = resultSet.getString("dqlx1cnt");
                                String ygh = resultSet.getString("ygh");
                                String wcdbbl = resultSet.getString("wcdbbl");
                                String data_2 = UuidUtil.getData_2(wcdbbl);
                                SimpleDateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                String dateStr = dateformat.format(System.currentTimeMillis());
                                logger.debug("维持担保比例告警，yyb" + yybmc);
                                String qujian = dqlx1cnt.split("在")[1].trim();
                                assignXX_only_dd(accessToken, ygh, yybmc + ", 你部客户" + khxm + "（客户号：" + khh + "）目前信用账户维持担保比例为" + data_2 + "%，处于" + qujian + "，请前往风险监控平台——预警中心查看处理。 " + dateStr);
                            } else if ("2".equals(bz)) {
                                String yybmc = resultSet.getString("yybmc");
                                String khxx = resultSet.getString("khxx");
                                String mobile = resultSet.getString("mobile");
                                String ygh = resultSet.getString("ygh");
                                SimpleDateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss ");
                                String dateStr = dateformat.format(System.currentTimeMillis());
                                logger.debug("合同到期告警，yyb" + yybmc);
                                String message = alertHtdqMessage(khxx);
                                assignXX_only_dd(accessToken, ygh, yybmc + "," + message + dateStr);
                                //TODO 将发送信息写入表中
//                                write_ff(khxx, ygh);
                            } else if ("3".equals(bz)) {
                                String yybmc = resultSet.getString("yybmc");
                                String khxx = resultSet.getString("khxx");
                                String mobile = resultSet.getString("mobile");
                                String ygh = resultSet.getString("ygh");
                                SimpleDateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss ");
                                String dateStr = dateformat.format(System.currentTimeMillis());
                                logger.debug("专项合同到期告警，yyb" + yybmc);
                                String message = alertHtdqMessage(khxx);
                                assignXX_only_dd(accessToken, ygh, yybmc + "," + message + dateStr);
//                                write_ff(khxx, ygh);
                            }
                        }
                    }
                }
            }
            long end_time = System.currentTimeMillis();
            long time_diff = end_time - begin_time;
            logger.debug("-------执行总耗时: ------" + String.valueOf(time_diff));
//            conn.commit();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
//            if (!resultSet.isClosed()) {
//                resultSet.close();
//            }
//            if (!stmt.isClosed()) {
//                stmt.close();
//            }
        }
        return resultSet;
    }

    public void write_ff(String khxx, String gyh) {
        String[] split = khxx.split(",");
        for (String mess : split) {
            String dqlx_mess = null;
            String khh = null;
            String khxm = null;
            try {
                String[] split_mess = mess.split("\\|");
                dqlx_mess = split_mess[0];
                khh = split_mess[1];
                khxm = split_mess[2];
            } catch (ArrayIndexOutOfBoundsException e) {
                dqlx_mess = "00";
                khh = "00";
                khxm = "00";
                System.out.println("数据格式错误，请检查！" + e.getMessage());
                String accessToken = null;
                try {
                    accessToken = getAccessToken();
                    assignXX_only_dd(accessToken, "002968", "数据格式错误，请检查。" + mess);
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
            String sql = "insert into margin_txy_mess_record(khh,khxm,gjlx,rece,rq,time) values (?,?,?,?,?,?)";
            PreparedStatement pst = null;
            try {
                connection = mySQLUtil.getConnection();
                //用来执行SQL语句查询，对sql语句进行预编译处理
                SimpleDateFormat sdf = new SimpleDateFormat();// 格式化时间
                sdf.applyPattern("yyyyMMdd HH:mm:ss");// a为am/pm的标记
                java.util.Date date = new Date();// 获取当前时间
                String format = sdf.format(date);
                String[] s = format.split(" ");
                String rq = s[0];
                String time = s[1];
                pst = connection.prepareStatement(sql);
                pst.setString(1, khh);
                pst.setString(2, khxm);
                pst.setString(3, dqlx_mess);
                pst.setString(4, gyh);
                pst.setString(5, rq);
                pst.setString(6, time);
                pst.executeUpdate();
            } catch (SQLException e) {
                System.out.println(e.getMessage());
            } finally {
                try {
                    if (!pst.isClosed()) {
                        pst.close();
                    }
                    if (!connection.isClosed()) {
                        connection.close();
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
