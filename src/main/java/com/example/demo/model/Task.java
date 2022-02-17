package com.example.demo.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import javax.persistence.*;

/**
 * @author LH
 * @description:
 * @date 2021-07-21 16:26
 */
@Entity
@Table(name = "result")
@JsonIgnoreProperties({"handler","hibernateLazyInitializer"})
public class Task {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    int id;
    String task_id;
    String expression;
    String remark;
    String flage;
    String dx;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getTask_id() {
        return task_id;
    }

    public void setTask_id(String task_id) {
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

    public String getFlage() {
        return flage;
    }

    public void setFlage(String flage) {
        this.flage = flage;
    }

    public String getDx() {
        return dx;
    }

    public void setDx(String dx) {
        this.dx = dx;
    }
}
