!set plsqlUseSlash true
CREATE OR REPLACE PROCEDURE cust.p_ids_run_all(
    I_KSRQ INT,
    I_JSRQ INT
)
IS
    V_START timestamp;
    V_END timestamp;
    V_FIRST timestamp;
    TYPE cur_jyr IS REF CURSOR;
    V_CUR cur_jyr;
    V_RQ INT;
BEGIN

    IF I_JSRQ IS NULL THEN
        I_JSRQ := I_KSRQ;
    end IF;
    OPEN V_CUR FOR  SELECT
                     DISTINCT jyr
                 FROM
                     dsc_cfg.t_xtjyr WHERE jyr BETWEEN I_KSRQ AND I_JSRQ ORDER BY jyr asc;
        LOOP
          
         FETCH V_CUR INTO V_RQ
         EXIT WHEN V_CUR%NOTFOUND;
        SELECT current_timestamp() INTO V_FIRST FROM system.dual;
        
        set_env('hive.correlated.subquery.ast.transform', false);
        --1.���н���ÿ������
        BEGIN 
            SELECT current_timestamp() INTO V_START FROM system.dual;
            CUST.P_IDS_RUNNER(V_RQ,NULL);
            SELECT current_timestamp() INTO V_END FROM system.dual;
            P_IDS_RECORD_PROC_LOG('P_IDS_RUNNER', V_START, V_END, V_RQ);
        END;

        --2.������ȯÿ������
        BEGIN 
            SELECT current_timestamp() INTO V_START FROM system.dual;
            P_IDS_XY_RUNNER(V_RQ,NULL);
            SELECT current_timestamp() INTO V_END FROM system.dual;
             P_IDS_RECORD_PROC_LOG('P_IDS_XY_RUNNER', V_START, V_END, V_RQ);
        END;

        --3.������Ȩÿ������
        BEGIN 
            SELECT current_timestamp() INTO V_START FROM system.dual;
            P_IDS_SO_RUNNER(V_RQ,NULL);
            SELECT current_timestamp() INTO V_END FROM system.dual;
            P_IDS_RECORD_PROC_LOG('P_IDS_SO_RUNNER', V_START, V_END, V_RQ);
        END;
        
        --4.���ʵ�ͳ��
        BEGIN 
            SELECT current_timestamp() INTO V_START FROM system.dual;
            P_IDS_DAILY_BILL_NJZQ(V_RQ,NULL);
            SELECT current_timestamp() INTO V_END FROM system.dual;
            P_IDS_RECORD_PROC_LOG('P_IDS_DAILY_BILL_NJZQ', V_START, V_END, V_RQ);
        END;
       --5.ӯ������
        BEGIN 
            SELECT current_timestamp() INTO V_START FROM system.dual;
            P_IDS_KHFX_YKPM(V_RQ,NULL);
            SELECT current_timestamp() INTO V_END FROM system.dual;
            P_IDS_RECORD_PROC_LOG('P_IDS_KHFX_YKPM', V_START, V_END, V_RQ);
        END;
       
        --6.���ݵ���
        set_env('hive.correlated.subquery.ast.transform', true);
        BEGIN
            SELECT current_timestamp() INTO V_START FROM system.dual;
            P_IDS_IMPORT_HYPERBASE(V_RQ,NULL);
            SELECT current_timestamp() INTO V_END FROM system.dual;
             P_IDS_RECORD_PROC_LOG('P_IDS_IMPORT_HYPERBASE', V_START, V_END, V_RQ);
        END;
        
        
        --7.����������ʱ��
        BEGIN
	        SELECT current_timestamp() INTO V_START FROM system.dual;
	        P_IDS_DROP_ALL_TEMP();
	        SELECT current_timestamp() INTO V_END FROM system.dual;
            P_IDS_RECORD_PROC_LOG('P_IDS_DROP_ALL_TEMP', V_START, V_END, V_RQ);
	    END;
        SELECT current_timestamp() INTO V_END FROM system.dual;
        P_IDS_RECORD_PROC_LOG('P_IDS_RUN_ALL', V_FIRST, V_END, V_RQ);
               
    END LOOP;
END;
/