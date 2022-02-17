package com.example.demo.service;

import com.example.demo.dao.MessageDAO;
import com.example.demo.model.Message;
import com.example.demo.until.ddmessage.AssignMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @author LH
 * @description:
 * @date 2021-09-10 16:19
 */
@Service
public class MessageService {
    AssignMessage assignMessage = new AssignMessage();
    @Autowired(required = false)
    MessageDAO messageDAO;

    public List<Message> list() {
        List<Message> users = messageDAO.findAll();
        return users;
    }

//    public boolean isExist(Message message) {
//        String task_name = message.getTask_name();
//        String content = message.getContent();
//        String rq= message.getRq();
//        Message messa = messageDAO.getByNameAndContenAndRq(task_name,content,rq);
//        return null != messa;
//    }

//    public Message findByUsername(String name) {
//        return messageDAO.findByTask_name(name);
//    }

//    public Message get(String name, int age) {
//        return messageDAO.getByNameAndAge(name, age);
//    }

    public int register(Message message) {
        String task_name = message.getTask_name();
        String content = message.getContent();
        if (task_name.equals("") || content.equals("")) {
            return 0;
        }
//        boolean exist = isExist(message);
//        if (exist) {
//            return 2;
//        }
        messageDAO.save(message);
        return 1;
    }

    public void updateUserStatus(Message user) {
//        Message userInDB = messageDAO.findByKHH(user.getKhh());
//        messageDAO.save(userInDB);
    }

    public StringBuffer fsdx(String user_id, String yyb) {
        StringBuffer sb = assignMessage.assignXX_only_dd_task(user_id, yyb);
        return sb;
    }
}
