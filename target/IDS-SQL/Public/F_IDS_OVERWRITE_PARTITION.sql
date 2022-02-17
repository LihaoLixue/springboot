!set plsqlUseSlash true
CREATE OR REPLACE FUNCTION CUST.F_IDS_OVERWRITE_PARTITION(
  I_TABLENAME IN STRING,
  I_DBNAME IN STRING,
  I_TARTABLENAME IN STRING,
  I_RQ IN INT,
  I_KHH IN STRING
)
RETURN BOOLEAN
IS
  TYPE NEST_TABLE IS TABLE OF STRING;
  l_tarTableColumns NEST_TABLE DEFAULT NULL;
  l_tmpTableColumns NEST_TABLE DEFAULT NULL;
  l_selectSql STRING;
  l_instertSql STRING;
  l_execSql STRING;
  l_partitionKey STRING;
  l_diff INT;
  l_addString STRING;
  l_deleteSql STRING;
BEGIN
  /**
   * 1.获取临时表所有列
   * 2.获取目标表所有列
   * 3.判断临时表列数是否与目标表一致
   * 4.拼接select与insert语句
   * 5.针对列数不一致的，临时表用null补齐
   */
   
   -- 1.获取临时表所有列
   l_selectSql := '';
   l_tmpTableColumns := get_columns(I_TABLENAME);
   
   FOR indx IN l_tmpTableColumns.first() .. l_tmpTableColumns.last()-1 LOOP
     l_selectSql := l_selectSql || l_tmpTableColumns(indx) || ',';
   END LOOP;
   
   -- 2.获取目标表所有列
   --l_insertSql := '';
   l_tarTableColumns := get_columns(I_DBNAME || '.' || I_TARTABLENAME);
   
   FOR indx IN l_tarTableColumns.first() .. l_tarTableColumns.last()-1 LOOP
     l_instertSql := l_instertSql || l_tarTableColumns(indx) || ',';
   END LOOP;
   
   -- 3.获取分区字段
   l_partitionKey := '';
   SELECT column_name  INTO l_partitionKey FROM `SYSTEM`.partition_keys_v WHERE table_name = LOWER(I_TARTABLENAME) AND database_name = LOWER(I_DBNAME) LIMIT 1;
   -- 3.判断列数是否一致
--   IF l_tmpTableColumns.COUNT() >= l_tarTableColumns.COUNT() THEN
   l_execSql := 'INSERT INTO TABLE ' ||
                  I_DBNAME || '.' || I_TARTABLENAME ||
                  ' PARTITION(' || l_partitionKey ||
                  '=' || I_RQ || ') (' ||
                  substr(l_selectSql, 1, oracle_instr(l_selectSql, ',', -1)-1)  || 
                  ') SELECT ' || 
                  substr(l_selectSql, 1, oracle_instr(l_selectSql, ',', -1)-1) ||
                  ' FROM ' || I_TABLENAME || ';';
   /*ELSIF l_tmpTableColumns.COUNT() < l_tarTableColumns.COUNT() THEN
     l_diff := l_tarTableColumns.COUNT() - l_tmpTableColumns.COUNT();
     l_addString := '';
     FOR indx IN l_tmpTableColumns.first() .. l_tmpTableColumns.last()-1 LOOP
        l_addString := l_addString || l_tarTableColumns(indx) || ',';
     END LOOP;
     FOR i IN 1 .. l_diff LOOP
       l_addString := l_addString || 'null as ' || l_tarTableColumns(l_tmpTableColumns.COUNT() + i) || ',';
     END loop;
     l_execSql := 'INSERT INTO TABLE ' ||
                  I_DBNAME || '.' || I_TARTABLENAME ||
                  ' PARTITION(' || l_partitionKey ||
                  '=' || I_RQ || ') (' ||
                  substr(l_instertSql, 1, oracle_instr(l_selectSql, ',', -1)-1)  || 
                  ') SELECT ' || 
                  substr(l_addString, 1, oracle_instr(l_selectSql, ',', -1)-1) ||
                  ' FROM ' || I_TABLENAME || ';';
                  
   END IF;*/
   
   IF I_KHH IS NULL THEN
     l_deleteSql := 'ALTER TABLE ' || I_DBNAME || '.' || I_TARTABLENAME || ' DROP PARTITION(' || l_partitionKey || '=' || I_RQ || ');';
   ELSE
     l_deleteSql := 'DELETE FROM ' || I_DBNAME || '.' || I_TARTABLENAME || ' PARTITION(' || l_partitionKey || '=' || I_RQ || ') WHERE KHH=' || I_KHH ||';';
   END IF;
    -- 根据
  EXECUTE IMMEDIATE l_deleteSql;
  EXECUTE IMMEDIATE l_execSql;
  RETURN TRUE;
END;
/
