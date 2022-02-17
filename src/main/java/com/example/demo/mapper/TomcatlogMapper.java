package com.example.demo.mapper;

import com.example.demo.model.WarCron;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * @author LH
 * @description:
 * @date 2021-06-29 15:22
 */
@Repository
@Mapper
public interface TomcatlogMapper {
    @Select("select * from cron_1 where flage=1")
    List<WarCron> queryScheduledTask();
}
