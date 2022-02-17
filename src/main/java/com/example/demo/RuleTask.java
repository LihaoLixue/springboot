package com.example.demo;


import com.example.demo.model.WarCron;
import com.example.demo.until.scheduler.ExecuteSQLUtil;
import net.minidev.json.JSONUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.*;
import org.springframework.scheduling.config.CronTask;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;

import javax.annotation.PreDestroy;
import java.sql.Connection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;

import org.apache.log4j.Logger;

/**
 * @author LH
 * @description:
 * @date 2021-06-30 8:36
 */
@Configuration
@EnableScheduling
public class RuleTask implements SchedulingConfigurer {
    Logger logger = Logger.getLogger(RuleTask.class);
    @Autowired
    ExecuteSQLUtil executeSQLUtil;
    private volatile ScheduledTaskRegistrar registrar;
    private ConcurrentHashMap<Long, ScheduledFuture<?>> scheduledFutures = new ConcurrentHashMap<>();
    private ConcurrentHashMap<Long, CronTask> cronTasks = new ConcurrentHashMap<>();
    private ConcurrentHashMap<Long, String> expressionTasks = new ConcurrentHashMap<>();

    @Override
    public void configureTasks(ScheduledTaskRegistrar registrar) {
        registrar.setScheduler(Executors.newScheduledThreadPool(30));
        this.registrar = registrar;
    }

    /**
     * 刷新任务
     * cronList是数据库查出来的定时任务列表
     *
     * @param cronList
     */
    public void refresh(List<WarCron> cronList) {
        cronList.stream().forEach(e -> logger.info("进入调度逻辑部分，具体任务如下: " + e.getTask_id()));
        //取消已经删除的任务
        Set<Long> cronIds = scheduledFutures.keySet();
        for (Long cronId : cronIds) {
            if (!exists(cronList, cronId)) {
                logger.debug("任务 " + cronId + " 取消了!");
                scheduledFutures.get(cronId).cancel(false);
                scheduledFutures.remove(cronId);
                cronTasks.remove(cronId);
                expressionTasks.remove(cronId);
            }
        }
        if (cronList != null) {
            for (WarCron warCron : cronList) {
                logger.info("1111111111111111111111111111111");
                String expression = warCron.getExpression();
                String remark = warCron.getRemark();
                //定时任务已存在且表达式未发生变化时跳过
                if (scheduledFutures.containsKey(warCron.getTask_id()) && cronTasks.get(warCron.getTask_id()).getExpression().equals(expression) && expressionTasks.get(warCron.getTask_id()).equals(remark)) {
                    logger.info("2222222222222222222222222222222222222222222222");
                    continue;
                }
                //如果执行时间发生了变化，则取消当前的定时任务
                if (scheduledFutures.containsKey(warCron.getTask_id())) {
                    logger.info("3333333333333333333333333333333333333333333333");
                    logger.debug("任务发生变化," + warCron.getTask_id());
                    scheduledFutures.get(warCron.getTask_id()).cancel(false);
                    scheduledFutures.remove(warCron.getTask_id());
                    cronTasks.remove(warCron.getTask_id());
                    expressionTasks.remove(warCron.getTask_id());
                }
                CronTask task = new CronTask(
                        new Runnable() {
                            @Override
                            public void run() {
                                Connection connection = null;
                                System.out.println("------------------------测试-测试-测试----------------------------------------");
                                String path = System.getProperty("user.dir");
                                String outpath = path + "/tasks/";
//                                connection = executeSQLUtil.executeSql(outpath + warCron.getRemark(),warCron.getDx());
                                try {
                                    executeSQLUtil.executeSql_1(outpath + warCron.getRemark(), warCron.getDx(), warCron.getSwitch_dx());
                                } catch (Exception e) {
                                    logger.error("sql文件执行出现问题，注意检查问题 " + e.getMessage());
                                    e.printStackTrace();
                                }
                                logger.debug("正在执行定时任务 " + warCron.getTask_id()+" 所执行任务名为 "+ warCron.getRemark());
                            }
                        }, expression
                );
                ScheduledFuture<?> future = registrar.getScheduler().schedule(task.getRunnable(), task.getTrigger());
                future.isDone();
                cronTasks.put(warCron.getTask_id(), task);
                expressionTasks.put(warCron.getTask_id(), remark);
                cronTasks.keySet().stream().forEach(e ->logger.info(e.toString()));
                scheduledFutures.put(warCron.getTask_id(), future);
            }
        }
    }

    /**
     * 判断是否有该任务
     *
     * @param warCronList
     * @param cronId
     * @return
     */
    private boolean exists(List<WarCron> warCronList, Long cronId) {
        for (WarCron warCron : warCronList) {
            if (cronId.equals(warCron.getTask_id())) {
                return true;
            }
        }
        return false;
    }

    @PreDestroy
    public void destroy() {
        this.registrar.destroy();
    }
}
