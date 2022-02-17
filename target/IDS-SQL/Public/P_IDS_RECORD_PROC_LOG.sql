DROP TABLE cust.t_proc_run_log;
CREATE TABLE cust.t_proc_run_log(
    log_id INT,
    proc_name STRING,
    start_time timestamp,
    end_time timestamp,
    RQ int 
)CLUSTERED  BY (RQ) INTO 1 BUCKETS
STORED AS ORC
tblproperties("transactional"="true");
DROP SEQUENCE cust.seq_run_log;
CREATE SEQUENCE cust.seq_run_log;
!set plsqlUseSlash true
CREATE OR REPLACE PROCEDURE P_IDS_RECORD_PROC_LOG(
    I_PROC IN STRING,
    I_START IN TIMESTAMP,
    I_END IN TIMESTAMP,
    I_RQ IN INT 
)
IS 
BEGIN
	INSERT INTO cust.t_proc_run_log VALUES(cust.seq_run_log.NEXTVAL, I_PROC , I_START, I_END, I_RQ);
END;
/