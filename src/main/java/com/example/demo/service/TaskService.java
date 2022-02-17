package com.example.demo.service;

import com.example.demo.dao.TaskDAO;
import com.example.demo.model.Task;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @author LH
 * @description:
 * @date 2021-07-21 16:48
 */
@Service
public class TaskService {
    @Autowired
    TaskDAO taskDAO;
    public List<Task> list() {
        Sort sort = Sort.by(Sort.Direction.DESC, "id");
        return taskDAO.findAll(sort);
    }

    public void addOrUpdate(Task task) {
        taskDAO.save(task);
    }
    public void deleteById(int id) {
        taskDAO.deleteById(id);
    }


//    public List<Task> Search(String keywords) {
//        return taskDAO.findAllByTitleLikeOrAuthorLike('%' + keywords + '%');
//    }
}
