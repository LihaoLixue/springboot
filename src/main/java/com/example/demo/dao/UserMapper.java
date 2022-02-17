package com.example.demo.dao;

import com.example.demo.model.Excel;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * @author LH
 * @description:
 * @date 2021-12-30 18:20
 */

@Mapper
public interface UserMapper {
    @Select("select yyb,type,wbqj,dqlx,khh,rq,qylx from alert_new_2")
    List<Excel> getAllUser();
}
