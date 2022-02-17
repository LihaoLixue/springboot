package com.example.demo.serviceimpl;

import com.example.demo.RuleTask;
import com.example.demo.mapper.TomcatlogMapper;
import com.example.demo.model.WarCron;
import com.example.demo.until.mysql.MySQLUtil;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Service;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author LH
 * @description:
 * @date 2021-06-30 8:53
 */
@Service
public class ApplicationRunnerImpl implements CommandLineRunner {
    static Logger logger = Logger.getLogger(RuleTask.class);
    //    @Autowired
//    protected SocketServer socketServer;
    @Autowired
    private RuleTask scheduledTask;
    @Autowired
    private TomcatlogMapper radioServer;
    private  static MySQLUtil mySQLUtil= new MySQLUtil();


    @Override
    public void run(String... args) throws Exception {
        logger.debug("启动boot成功");

        //todo 非交易日时需要增加睡眠时间
        while (true) {
            System.out.println("111111111111111111111111111111111111111");
            List<WarCron> warCrons = selectAll();
            for (int i = 0; i < warCrons.size(); i++) {
                WarCron warCron = warCrons.get(i);
//                logger.debug("id is " + warCron.getTask_id()+ " expression is "+ warCron.getExpression()+" remark is "+ warCron.getRemark()+" flage is "+ warCron.getFlage());
            }
            scheduledTask.refresh(warCrons);
            Thread.sleep(20000);
        }
    }

    public static List<WarCron> selectAll() {
        List<WarCron> list = new ArrayList<>();
//        ComboPooledDataSource dataSource = MySQLUtil.getConnection_1();
        String sql = "select * from cron where flage=1";
        Connection connection = null;
        ResultSet resultSet = null;
        PreparedStatement ps=null;
        try {
            connection = mySQLUtil.getConnection();
//            connection = dataSource.getConnection();
            ps = connection.prepareStatement(sql);
            resultSet = ps.executeQuery();
            while (resultSet.next()) {
                Long task_id = resultSet.getLong("task_id");
                String expression = resultSet.getString("expression");
                String remark = resultSet.getString("remark");
                String flage = resultSet.getString("flage");
                String dx = resultSet.getString("dx");
                String main_id = resultSet.getString("main_id");
                String switch_dx = resultSet.getString("switch_dx");
                WarCron warCron = new WarCron();
                warCron.setTask_id(task_id);
                warCron.setExpression(expression);
                warCron.setRemark(remark);
                warCron.setFlage(flage);
                warCron.setDx(dx);
                warCron.setMain_id(main_id);
                warCron.setSwitch_dx(switch_dx);
                list.add(warCron);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
//            logger.info("41111111");
//            try {
//                if(!resultSet.isClosed()){
//                    logger.info("1111111");
//                    resultSet.close();
//                }
//                if(!ps.isClosed()){
//                    logger.info("21111111");
//                    ps.close();
//                }
//                if(!connection.isClosed()){
//                    logger.info("31111111");
//                    connection.close();
//                }
//            } catch (SQLException e) {
//                e.printStackTrace();
//            }
        }
        return list;
    }
}
