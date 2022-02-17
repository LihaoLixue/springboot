package com.example.demo.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Builder;

import javax.persistence.*;

/**
 * @author LH
 * @description: 反馈信息表
 * @date 2021-09-10 16:02
 */
@Builder
@Entity
@Table(name = "message_api")
@JsonIgnoreProperties({"handler","hibernateLazyInitializer"})
public class Message {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column
   //递增主键
    int id;
    @Column
            //任务名称
    String task_name;
    @Column
            //任务内容
    String content;
    @Column
            //任务分配时间
    String task_assig_time;
    @Column
            //任务截止时间
    String task_due_time;
    @Column
            //任务接收人
    String task_rece;
    @Column
            //
    String task_stat;
    @Column
    String rq;
    @Column
    String yyb;

    public Message() {
    }

    public Message(int id, String task_name, String content, String task_assig_time, String task_due_time, String task_rece, String task_stat, String rq, String yyb) {
        this.id = id;
        this.task_name = task_name;
        this.content = content;
        this.task_assig_time = task_assig_time;
        this.task_due_time = task_due_time;
        this.task_rece = task_rece;
        this.task_stat = task_stat;
        this.rq = rq;
        this.yyb = yyb;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getTask_name() {
        return task_name;
    }

    public void setTask_name(String task_name) {
        this.task_name = task_name;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getTask_assig_time() {
        return task_assig_time;
    }

    public void setTask_assig_time(String task_assig_time) {
        this.task_assig_time = task_assig_time;
    }

    public String getTask_due_time() {
        return task_due_time;
    }

    public void setTask_due_time(String task_due_time) {
        this.task_due_time = task_due_time;
    }

    public String getTask_rece() {
        return task_rece;
    }

    public void setTask_rece(String task_rece) {
        this.task_rece = task_rece;
    }

    public String getTask_stat() {
        return task_stat;
    }

    public void setTask_stat(String task_stat) {
        this.task_stat = task_stat;
    }

    public String getRq() {
        return rq;
    }

    public void setRq(String rq) {
        this.rq = rq;
    }

    public String getYyb() {
        return yyb;
    }

    public void setYyb(String yyb) {
        this.yyb = yyb;
    }
}
