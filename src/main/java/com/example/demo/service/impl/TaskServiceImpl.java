package com.example.demo.service.impl;

import com.example.demo.dao.TaskDAO;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

/**
 * @author LH
 * @description:
 * @date 2021-09-23 17:33
 */
@Service
public class TaskServiceImpl  {
    @Resource
    private TaskDAO dao;



}
