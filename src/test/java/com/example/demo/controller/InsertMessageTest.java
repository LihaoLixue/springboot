//package com.example.demo.controller;
//
//import com.example.demo.dao.MessageDAO;
//import com.example.demo.model.Message;
//import com.example.demo.service.MessageService;
//import org.junit.jupiter.api.BeforeEach;
//import org.junit.jupiter.api.Test;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.mock.web.MockHttpSession;
//
//import static org.junit.jupiter.api.Assertions.*;
//
///**
// * @author LH
// * @description:
// * @date 2021-09-16 15:17
// */
//class InsertMessageTest {
//
//
//        @Autowired
//        MessageService messageService;
//        @Autowired
//        MessageDAO dao;
//        private Message mvc;
//        private MockHttpSession session;
//
//        @BeforeEach
//        void setUp() {
//            mvc = Message.builder().name("lihao").age(10).build();
//        }
//    @Test
//    void login() {
//            messageService.register()
//    }
//}
