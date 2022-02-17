!set plsqlUseSlash true
create or replace procedure cust.p_ids_drop_all_temp() is

/******************************************************************
  *文件名称：CUST.P_IDS_DROP_ALL_TEMP
  *项目名称：IDS计算
  *文件说明：集中交易-删除临时表

  创建人：胡阳明
  功能说明：集中交易-删除临时表

  参数说明

  修改者        版本号        修改日期        说明
  胡阳明        v1.0.0        2019/6/27       创建
*******************************************************************/
    drop_table STRING;
    CURSOR cur IS SELECT concat(database_name,'.', table_name) AS drop_table  FROM system.tables_v WHERE database_name='tempspark';
BEGIN
    /*BEGIN
        EXECUTE IMMEDIATE 'DROP DATABASE IF EXISTS tempspark CASCADE;';
		EXECUTE IMMEDIATE 'CREATE DATABASE IF NOT EXISTS tempspark;';
    END;*/

    BEGIN
        OPEN cur;
        LOOP
           FETCH cur INTO drop_table
           EXIT WHEN cur%NOTFOUND
           EXECUTE IMMEDIATE "DROP TABLE " || drop_table;
        END LOOP;
        CLOSE cur;
    END;
end;
/
