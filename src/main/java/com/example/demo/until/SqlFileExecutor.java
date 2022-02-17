package com.example.demo.until;
import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * @author LH
 * @description:读取 SQL 脚本文件并解析
 * @date 2021-09-06 13:47
 */

public class SqlFileExecutor {
   static Logger logger = Logger.getLogger(SqlFileExecutor.class);

    /**
     * 读取 SQL 文件，获取 SQL 语句
     * @param sqlFile SQL 脚本文件
     * @return List<sql> 返回所有 SQL 语句的 List
     * @throws Exception
     */
    private static List<String> loadSql(String sqlFile) throws Exception {
        List<String> sqlList = new ArrayList<String>();
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
                    sqlList.add(sql);
                }
            }
            return sqlList;
        } catch (Exception ex) {
            throw new Exception(ex.getMessage());
        }
    }

    /**
     * 传入连接来执行 SQL 脚本文件，这样可与其外的数据库操作同处一个事物中
     * @param conn 传入数据库连接
     * @param sqlFile SQL 脚本文件
     * @throws Exception
     */
    public ResultSet execute(Connection conn, String sqlFile) throws Exception {
        Statement stmt = null;
        List<String> sqlList = loadSql(sqlFile);
        stmt = conn.createStatement();
        ResultSet resultSet =null;
        for (String sql : sqlList) {
            resultSet = stmt.executeQuery(sql);
        }
       return resultSet;
    }

    /**
     * 自建连接，独立事物中执行 SQL 文件
     * @param sqlFile SQL 脚本文件
     * @throws Exception
     */
    public static ResultSet execute(String sqlFile,String dx) throws Exception {
        Connection conn = DBCenter.getConnection();
        ResultSet resultSet =null;
        Statement stmt = null;
        List<String> sqlList = loadSql(sqlFile);
        try {
            conn.setAutoCommit(true);
            stmt = conn.createStatement();
            for (String sql : sqlList) {
               if (sql.startsWith("insert")||sql.startsWith("INSERT")){
                   logger.debug("%%%%%%%%%%%%%%%%%%%执行插入sql"+ sql);
                   boolean execute = stmt.execute(sql);
                   logger.debug("%%%%%%%%%%%%%%%%%%%执行结果"+ execute);
               }else if(sql.startsWith("SELECT")||sql.startsWith("select")){
                   resultSet = stmt.executeQuery(sql);
                   while (resultSet.next()){
                       logger.debug("%%%%%%%%%%%%%%%%%%%执行结果"+ resultSet.getString("khh"));
                   }
               }
            }
//            DBCenter.commit(conn);
        } catch (Exception ex) {
//            DBCenter.rollback(conn);
            throw ex;
        } finally {
            conn.close();
            stmt.close();
//            DBCenter.close(null, stmt, conn);
        }
        return resultSet;
    }

    public static void main(String[] args) throws Exception {
        List<String> sqlList = new SqlFileExecutor().loadSql(args[0]);
        System.out.println("size:" + sqlList.size());
        for (String sql : sqlList) {
            System.out.println(sql);
        }
    }
}
