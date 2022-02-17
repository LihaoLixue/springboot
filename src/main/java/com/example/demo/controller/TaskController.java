//package com.example.demo.controller;
//
//import com.example.demo.model.Task;
//import com.example.demo.service.TaskService;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.web.bind.annotation.*;
//
//import java.util.List;
//
///**
// * @author LH
// * @description:
// * @date 2021-07-21 16:50
// */
//@RestController
//public class TaskController {
//    @Autowired
//    TaskService taskService;
//
//    @GetMapping("/api/task")
//    public List<Task> list() throws Exception {
//        return taskService.list();
//    }
//
//    @PostMapping("/api/task")
//    public Task addOrUpdate(@RequestBody Task task) throws Exception {
//        taskService.addOrUpdate(task);
//        return task;
//    }
//    @PostMapping("/api/delete")
//    public void delete(@RequestBody Task task) throws Exception {
//        taskService.deleteById(task.getId());
//    }
//}
