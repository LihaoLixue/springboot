package com.example.demo.dao;

/**
 * @author LH
 * @description:
 * @date 2021-07-23 15:59
 */

import com.example.demo.model.User;
import org.springframework.data.jpa.repository.JpaRepository;

public interface UserDAO extends JpaRepository<User,Integer> {
    User findByUsername(String username);

    User getByUsernameAndPassword(String username,String password);
}
