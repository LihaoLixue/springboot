!set plsqlUseSlash true
-- 创建建立视图的函数
-- i_sqlBuf：查询sql的结果集
-- i_tableName：建临时表的名称
CREATE OR REPLACE FUNCTION CUST.F_IDS_CREATE_TEMP_TABLE(
    i_sqlBuf IN STRING, 
    i_tableName IN STRING
)
RETURN BOOLEAN
IS
BEGIN
    EXECUTE IMMEDIATE 'drop table if exists '|| i_tableName ||';';
    EXECUTE IMMEDIATE 'CREATE TABLE ' || i_tableName || ' STORED AS HOLODESK AS ' || i_sqlBuf || ';';
    RETURN TRUE;
END;
/
