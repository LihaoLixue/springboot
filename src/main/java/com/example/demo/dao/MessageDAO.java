package com.example.demo.dao;

import com.example.demo.model.Message;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * @author LH
 * @description:
 * @date 2021-09-10 16:17
 */
public interface MessageDAO extends JpaRepository<Message,Integer> {
//    Message findByTask_name(String task_name);

//    Message getByNameAndAge(String name,int age);
//    Message getByNameAndContenAndRq(String task_name,String content,String rq);

}
