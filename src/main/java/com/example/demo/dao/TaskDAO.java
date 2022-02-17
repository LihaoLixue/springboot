package com.example.demo.dao;

/**
 * @author LH
 * @description:
 * @date 2021-07-20 16:56
 */




import com.example.demo.model.Task;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface TaskDAO extends JpaRepository<Task,Integer> {
//    List<Task> findAllByName(String name);
//    List<Task> findAllByTitleLikeOrAuthorLike(String keyword1, String keyword2);
}
