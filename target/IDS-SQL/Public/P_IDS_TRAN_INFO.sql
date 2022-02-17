!set plsqlUseSlash true
CREATE OR REPLACE PROCEDURE CUST.P_IDS_TRAN_INFO(
                            i_ksrq in int--开始日期
)is
/*********************************************************************************************
    *文件名称：CUST.P_IDS_TRAN_INFO
    *项目名称：IDS计算
    *文件说明：金融数据同步

    创建人：王睿驹
    功能说明：金融数据同步
    使用该过程需要提前建立连接mysql的dblink名称MYSQL_INFO！
    修改者            版本号            修改日期            说明
    王睿驹            v1.0.0            2019/6/20            创建
*********************************************************************************************/
DECLARE 
    l_suffix STRING;
    l_DB STRING;
    l_dbname STRING;
    l_rq INT;
    l_count INT;
    l_temp_table STRING;
    l_sqlBuf STRING;
    l_fields_full STRING;--转码全字段
    l_fields_nosyn STRING;--不转码排除sync_col字段
    l_field_tran STRING;--转码函数添加到普通字段上
    CURSOR c_job  IS select table_name,cast(sync_type as int) sync_t,sync_col,cast(jyr_dif as int) jyr_d from pub_sys.t_sync_table_aliyun@MYSQL_INFO order by seq;
    v_table_name STRING;
    v_type int;
    v_col STRING;
    v_dif int;
BEGIN
    l_DB:='info';
    l_dbname:='tempspark.';
    BEGIN       
        OPEN c_job;
        LOOP FETCH c_job into v_table_name,v_type,v_col,v_dif;
        EXIT WHEN c_job%NOTFOUND;
            BEGIN               
                DECLARE
                CURSOR c_fields(table_name_in STRING) is select COLUMN_name from information_schema.`COLUMNS`@MYSQL_INFO where table_schema='info' and table_name=table_name_in;
                v_column_name STRING;   
                BEGIN
                    OPEN c_fields(v_table_name);
                    l_fields_full:='';
                    l_fields_nosyn:='';
                    LOOP FETCH c_fields into v_column_name;
                    EXIT WHEN c_fields%NOTFOUND;        
                            IF LOWER(v_column_name)="jys" THEN  --转码
                                l_field_tran:="F_GET_ETL_TRAN_DICVAL('DSC_BAS','T_BJHG_DYMX','JYS',1,1,"||v_column_name||")as "||v_column_name;
                            ELSEIF LOWER(v_column_name)="tadm"  THEN
                                l_field_tran:="F_GET_ETL_TRAN_DICVAL('DSC_BAS','T_BJHG_DYMX','JYS',1,1,"||v_column_name||")as "||v_column_name;
                            ELSEIF LOWER(v_column_name)="bz" THEN 
                                l_field_tran:="F_GET_ETL_TRAN_DICVAL('DSC_BAS','T_FP_CPFE','BZ',1,1,"||v_column_name||")as "||v_column_name;
                            ELSE
                                l_field_tran:=v_column_name;
                            END IF;
                            l_fields_full:=l_fields_full||l_field_tran||',';
                            --IF v_column_name!=v_col THEN
                                l_fields_nosyn:=l_fields_nosyn||v_column_name||',';--不转码排除sync_col字段                            
                            --END IF;
                    END LOOP;
                    CLOSE c_fields;
                    EXCEPTION
                    WHEN OTHERS THEN 
                        CLOSE c_fields;             
                END;
                l_suffix:='';
                IF v_type=2 THEN
                    l_rq:=F_GET_JYR_DATE(i_ksrq,0-v_dif);
                    l_suffix:=" where "||v_col||" between "||l_rq||" and "||i_ksrq;
                END IF;
                l_sqlBuf:="select "||SUBSTRING(l_fields_full,1,LENGTH(l_fields_full)-1)||" from "||v_table_name||"@MYSQL_INFO "||l_suffix;                  
                l_temp_table:=l_dbname||'spark'||v_table_name;
                F_IDS_CREATE_TEMP_TABLE(l_sqlBuf,l_temp_table);     
                IF v_type=2 THEN--增量                        
                    EXECUTE IMMEDIATE "DELETE FROM "||l_DB||"."||v_table_name|| l_suffix;
                    
                    EXECUTE IMMEDIATE "insert into table "||l_DB||"."||v_table_name||"("||SUBSTRING(l_fields_nosyn,1,LENGTH(l_fields_nosyn)-1) ||") select "||SUBSTRING(l_fields_nosyn,1,LENGTH(l_fields_nosyn)-1)||" from "||l_temp_table;
                ELSEIF v_type=1 THEN--全量
                    EXECUTE IMMEDIATE "TRUNCATE TABLE " ||l_DB||"."||v_table_name;
                    EXECUTE IMMEDIATE "insert INTO table "||l_DB||"."||v_table_name||" select * from "||l_temp_table;
                END IF;
            END;    
        END LOOP;
        CLOSE c_job;
    END;
END;
/