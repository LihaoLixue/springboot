package com.example.demo.model;

/**
 * @author LH
 * @description:
 * @date 2021-06-30 8:37
 */

public class WarCron {
    //任务id
    private long task_id;
    //任务表达式
    private String expression;
    //sql文件名
    private String remark;
    //任务状态(1-运行/0-停止)
    private String flage;
    //是否发送短信
    private String dx;
    // 调度其他任务的主任务
    private String main_id;

    public String getMain_id() {
        return main_id;
    }

    public void setMain_id(String main_id) {
        this.main_id = main_id;
    }

    private String switch_dx;

    //任务名称
    private String name;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }


    public String getFlage() {
        return flage;
    }

    public void setFlage(String flage) {
        this.flage = flage;
    }



    public long getTask_id() {
        return task_id;
    }

    public void setTask_id(long task_id) {
        this.task_id = task_id;
    }

    public String getExpression() {
        return expression;
    }

    public void setExpression(String expression) {
        this.expression = expression;
    }

    public String getRemark() {
        return remark;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }

    public String getDx() {
        return dx;
    }

    public void setDx(String dx) {
        this.dx = dx;
    }

    public String getSwitch_dx() {
        return switch_dx;
    }

    public void setSwitch_dx(String switch_dx) {
        this.switch_dx = switch_dx;
    }
}
