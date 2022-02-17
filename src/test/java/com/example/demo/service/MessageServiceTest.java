package com.example.demo.service;

import com.example.demo.dao.MessageDAO;
import com.example.demo.model.Message;
import lombok.extern.log4j.Log4j2;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.stereotype.Component;
import org.springframework.test.context.junit4.SpringRunner;

import javax.transaction.Transactional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author LH
 * @description:
 * @date 2021-09-10 17:22
 */
@Log4j2
@RunWith(MockitoJUnitRunner.class)
//@RunWith(SpringRunner.class)
//@SpringBootTest
@Component
@SpringBootTest(classes = MessageServiceTest.class)
class MessageServiceTest {
    @Mock
    MessageService messageService;
    @Mock
    MessageDAO dao;

//    @Test
//    void register_null() {
////        Message message = Message.builder().name("lihao").age(10).build();
//        System.out.println("-------------------------------------------------------------------------------------------------------------------");
//        dao.save(message);
//        dao.flush();
//        messageService.register(message);
//        System.out.println(messageService.list());
//    }
}
