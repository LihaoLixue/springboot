!set plsqlUseSlash true
CREATE OR REPLACE PROCEDURE CUST.P_IDS_SO_RUNNER(
--输入变量
I_RQ IN INT,
I_KHH IN STRING
)
IS
/******************************************************************
  *文件名称：CUST.P_SO_IDS_RUNNER
  *项目名称：IDS计算
  *文件说明：个股期权-IDS调度

  创建人：燕居庆
  功能说明：IDS入口过程

  参数说明

  修改者        版本号        修改日期        说明
  燕居庆        v1.0.0        2019/6/25         创建
  王睿驹        v1.0.1        2019/8/20        根据java代码修改 
*******************************************************************/
V_START DOUBLE;    --创建表语句
V_END DOUBLE; --临时表名
V_IDS_START DOUBLE;
V_PERIOD DECIMAL(16,2); 
BEGIN
  -- 根据现有流程一步步进行调度
  SELECT CURRENT_TIMESTAMP() INTO V_IDS_START FROM SYSTEM.DUAL;
  
  --成本盈亏处理
  BEGIN
    SELECT CURRENT_TIMESTAMP() INTO V_START FROM SYSTEM.DUAL;
    P_IDS_SO_COST_CALCUTION(I_RQ, I_KHH);
    SELECT CURRENT_TIMESTAMP() INTO V_END FROM SYSTEM.DUAL;
    V_PERIOD := V_END - V_START;
    PUT_LINE('个股期权-成本盈亏处理完成，耗时：' || V_PERIOD || '秒');
  END;
  
  --资产日统计
  BEGIN
    SELECT CURRENT_TIMESTAMP() INTO V_START FROM SYSTEM.DUAL;
    P_IDS_SO_CUST_DAILY_STAT(I_RQ, I_KHH);
    SELECT CURRENT_TIMESTAMP() INTO V_END FROM SYSTEM.DUAL;
    V_PERIOD := V_END - V_START;
    PUT_LINE('个股期权-资产日统计处理完成，耗时：' || V_PERIOD || '秒');
  END;
  
  V_PERIOD := V_END - V_IDS_START;
  PUT_LINE('个股期权处理完成，总耗时：' || V_PERIOD || '秒');
END;
/