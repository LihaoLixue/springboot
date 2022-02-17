package com.example.demo.controller;

/**
 * @author LH
 * @description:
 * @date 2021-07-23 16:00
 */

import com.example.demo.model.Message;
import com.example.demo.result.Result;
import com.example.demo.result.ResultFactory;
import com.example.demo.service.MessageService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@Controller
public class InsertMessage {
    @Autowired
    MessageService messageService;
    @CrossOrigin
    @PostMapping(value = "/api/register")
    @ResponseBody
    public Result login(@RequestBody Message message) {
        int status = messageService.register(message);
        switch (status) {
            case 0:
                return ResultFactory.buildFailResult("标题与内容不能为空");
            case 1:
                return ResultFactory.buildSuccessResult("添加成功");
            case 2:
                return ResultFactory.buildFailResult("任务已存在");
        }
        return ResultFactory.buildFailResult("未知错误");
    }
    @GetMapping(value="/api/list")
    @CrossOrigin
    @ResponseBody
    public List<Message> getUserList(){
        return messageService.list();
    }

    @CrossOrigin
    @PostMapping(value = "/api/fsxx")
    @ResponseBody
    public StringBuffer fsdx(String user_id,String yyb) {
        if(user_id.isEmpty()||yyb.isEmpty()){

        }
        StringBuffer sb = messageService.fsdx(user_id,yyb);
        System.out.println("接口返回参数： "+sb);
        return sb;

    }
//    public int fsdx(@RequestBody String json) {
//        json= json.replaceAll("\"","\\\"");
//        int fsdx = messageService.fsdx(json);
//        return fsdx;
//
//
//    }
}


