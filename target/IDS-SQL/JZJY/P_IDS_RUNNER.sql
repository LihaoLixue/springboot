!set plsqlUseSlash true
CREATE OR REPLACE PROCEDURE cust.p_ids_runner(
--输入变量
I_RQ IN INT,
I_KHH IN STRING
)
IS
/******************************************************************
  *文件名称：CUST.P_IDS_RUNNER
  *项目名称：IDS计算
  *文件说明：集中交易-IDS调度

  创建人：燕居庆
  功能说明：IDS入口过程

  参数说明

  修改者        版本号        修改日期        说明
  燕居庆        v1.0.0        2019/6/25           创建
*******************************************************************/
V_START DOUBLE;    --创建表语句
V_END DOUBLE; --临时表名
V_IDS_START DOUBLE;
V_PERIOD DECIMAL(16,2); 
BEGIN
  -- 根据现有流程一步步进行调度
  SELECT CURRENT_TIMESTAMP() INTO V_IDS_START FROM SYSTEM.DUAL;
  
  --交割单清算后处理
  BEGIN
    SELECT CURRENT_TIMESTAMP() INTO V_START FROM SYSTEM.DUAL;
    P_IDS_DELIVERY_ORDER_PRODUCE(I_RQ, I_KHH);
    SELECT CURRENT_TIMESTAMP() INTO V_END FROM SYSTEM.DUAL;
    V_PERIOD := V_END - V_START;
    PUT_LINE('集中交易-交割单清算后处理完成，耗时：' || V_PERIOD || '秒');
  END;
  
  --成本盈亏修正处理
  BEGIN
    SELECT CURRENT_TIMESTAMP() INTO V_START FROM SYSTEM.DUAL;
    P_IDS_COST_CALCUATION(I_RQ, I_KHH);
    SELECT CURRENT_TIMESTAMP() INTO V_END FROM SYSTEM.DUAL;
    V_PERIOD := V_END - V_START;
    PUT_LINE('集中交易-成本盈亏修正处理完成，耗时：' || V_PERIOD || '秒');
  END;
  
  --金额产品成本盈亏计算
  BEGIN
    SELECT CURRENT_TIMESTAMP() INTO V_START FROM SYSTEM.DUAL;
    P_IDS_FINANCE_PRODUCT_COST(I_RQ, I_KHH);
    SELECT CURRENT_TIMESTAMP() INTO V_END FROM SYSTEM.DUAL;
    V_PERIOD := V_END - V_START;
    PUT_LINE('集中交易-金额产品成本盈亏计算处理完成，耗时：' || V_PERIOD || '秒');
  END;
  
  --资产日统计
  BEGIN
    SELECT CURRENT_TIMESTAMP() INTO V_START FROM SYSTEM.DUAL;
    P_IDS_CUST_DAILY_STAT(I_RQ, I_KHH);
    SELECT CURRENT_TIMESTAMP() INTO V_END FROM SYSTEM.DUAL;
    V_PERIOD := V_END - V_START;
    PUT_LINE('集中交易-资产日统计处理完成，耗时：' || V_PERIOD || '秒');
  END;
  
  --清除所有临时表
  BEGIN
    SELECT CURRENT_TIMESTAMP() INTO V_START FROM SYSTEM.DUAL;
    p_ids_drop_all_temp();
    SELECT CURRENT_TIMESTAMP() INTO V_END FROM SYSTEM.DUAL;
    V_PERIOD := V_END - V_START;
    PUT_LINE('集中交易-临时表清空处理完成，耗时：' || V_PERIOD || '秒');
  END;
  
  V_PERIOD := V_END - V_IDS_START;
  PUT_LINE('集中交易处理完成，总耗时：' || V_PERIOD || '秒');
END;
/