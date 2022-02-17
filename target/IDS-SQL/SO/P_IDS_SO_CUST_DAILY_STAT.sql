!set plsqlUseSlash true
create or replace procedure cust.p_ids_so_cust_daily_stat(
  --输入变量
  I_RQ IN INT,
  I_KHH IN STRING

) is

/******************************************************************
  *文件名称：CUST.p_ids_so_cust_daily_stat
  *项目名称：IDS计算
  *文件说明：个股期权-资产日统计

  创建人：刘永舜
  功能说明：个股期权-资产日统计

  参数说明

  修改者        版本号        修改日期        说明
  刘永舜        v1.0.0        2019/6/27      创建
  王睿驹        v1.0.1        2019/8/20         根据java代码修改
  燕居庆        v1.0.2        2019/9/16      对标java-ids 4858版本
                                                1.根据持仓方向计算证券市值
                                                2.去除合约到期注销统计
                                                3.当日盈亏采用总资产计算
  燕居庆        v1.0.3        2019/9/25      对标java-ids 4995版本
                                                1. 生成当日统计最终数据：当日份额调整为ZZC-ZFZ-DRYK
*******************************************************************/
l_sqlBuf STRING; --创建表语句
l_tableName STRING; --临时表名
l_sqlWhereCurrentDay STRING; 
l_sqlWhereLastDay STRING;
l_lastDay INT;
l_sql STRING;
l_hlcsHKD DECIMAL(12,6);
l_hlcsUSD DECIMAL(12,6);
l_zhlb STRING;
TYPE nest_table IS TABLE OF STRING;
l_tableArr nest_table DEFAULT NULL;
l_columns STRING;
BEGIN
  BEGIN
    --1.1 获取汇率参数
    SELECT CAST(F_GET_HLCS('2',I_RQ) AS DECIMAL(12,6)) INTO l_hlcsHKD FROM `SYSTEM`.dual;
    SELECT CAST(F_GET_HLCS('3',I_RQ) AS DECIMAL(12,6)) INTO l_hlcsUSD FROM `SYSTEM`.dual;
  END;
  -- 获取上一交易日
  SELECT F_GET_JYR_DATE(I_RQ, -1) INTO l_lastDay FROM system.dual;
  
  l_zhlb:=' and  zhlb = "5"';

  IF I_KHH IS NULL THEN
    l_sqlWhereLastDay := l_lastDay;
    l_sqlWhereCurrentDay := I_RQ;
  ELSE 
    l_sqlWhereLastDay := l_lastDay || ' and khh = ' || I_KHH;
    l_sqlWhereCurrentDay := I_RQ || ' and khh = ' || I_KHH;
  END IF;
  
  BEGIN
  ----------------------------------------//资金余额-----------------------------------------------------
    /**
     * 临时表名
     * 由于集中/两融/期权可能存在相同临时表，因此在改造时，需要添加相关前缀：
     * 集中：F_IDS_GET_TABLENAME('sparkZjye', I_KHH);
     * 两融：F_IDS_GET_TABLENAME('xy_sparkZjye', I_KHH);
     * 期权：F_IDS_GET_TABLENAME('so_sparkZjye', I_KHH);
     */
    l_tableName := F_IDS_GET_TABLENAME('so_sparkZjye', I_KHH);
    l_sqlBuf := 'select * from CUST.t_so_zjye_his D where rq = ' || l_sqlWhereCurrentDay;
    
    /**
     * 调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
     */
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName)
  END;

  BEGIN
  ----------------------------证券余额------------------------------------
    /**
     * 临时表名
     * 由于集中/两融/期权可能存在相同临时表，因此在改造时，需要添加相关前缀：
     * 集中：F_IDS_GET_TABLENAME('sparkZqyeZctj', I_KHH);
     * 两融：F_IDS_GET_TABLENAME('xy_sparkZqyeZctj', I_KHH);
     * 期权：F_IDS_GET_TABLENAME('so_sparkZqyeZctj', I_KHH);
     */
    l_tableName := F_IDS_GET_TABLENAME('so_sparkZqyeZctj', I_KHH);
    l_sqlBuf := 'select khh,gdh,zzhbm,jys,hydm,hymc,bz,kcrq,qqlx,ccfx,bdbq,zqsl,abs(zxsz) as zxsz,kcsl,kcje,pcsl,pcje,cccb,cbj,ljyk,tbcccb,tbcbj,dryk,rq from cust.t_so_zqye_his where rq = ' || l_sqlWhereCurrentDay;
    
    /**
     * 调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
     */
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
  END;
 
   
  BEGIN
    /**
     * 临时表名
     * 由于集中/两融/期权可能存在相同临时表，因此在改造时，需要添加相关前缀：
     * 集中：F_IDS_GET_TABLENAME('sparkHyhq', I_KHH);
     * 两融：F_IDS_GET_TABLENAME('xy_sparkHyhq', I_KHH);
     * 期权：F_IDS_GET_TABLENAME('so_sparkHyhq', I_KHH);
     */
    l_tableName := F_IDS_GET_TABLENAME('so_sparkHyhq', I_KHH);
    l_sqlBuf := 'select * from cust.t_so_hyhq_his where rq = ' || I_RQ;
    
    /**
     * 调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
     */
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
  END;  
  
    BEGIN
    /**
     * 临时表名
     * 由于集中/两融/期权可能存在相同临时表，因此在改造时，需要添加相关前缀：
     * 集中：F_IDS_GET_TABLENAME('sparkHyccPre1', I_KHH);
     * 两融：F_IDS_GET_TABLENAME('xy_sparkHyccPre1', I_KHH);
     * 期权：F_IDS_GET_TABLENAME('so_sparkHyccPre1', I_KHH);
     */
    l_tableName := F_IDS_GET_TABLENAME('so_sparkHyccPre1', I_KHH);
    l_sqlBuf := 'select * from CUST.t_so_zqye_his D where rq = ' || l_sqlWhereLastDay;--  -1日
    
    /**
     * 调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
     */
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName)
  END;
  
----------------------------------------//上日资产统计------------------------------------------------  
  BEGIN
    /**
     * 临时表名
     * 由于集中/两融/期权可能存在相同临时表，因此在改造时，需要添加相关前缀：
     * 集中：F_IDS_GET_TABLENAME('sparkZctjSr', I_KHH);
     * 两融：F_IDS_GET_TABLENAME('xy_sparkZctjSr', I_KHH);
     * 期权：F_IDS_GET_TABLENAME('so_sparkZcSr', I_KHH);
     */
    l_tableName := F_IDS_GET_TABLENAME('so_sparkZcSr', I_KHH);
    l_sqlBuf := 'select * from cust.t_stat_so_ZC_R where rq = ' || l_sqlWhereLastDay;
    
    /**
     * 调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
     */
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
  END;
    
  BEGIN
    
    -----------------//资金存取------------------------------
    
    l_tableName := F_IDS_GET_TABLENAME('so_sparkZjcq', I_KHH);
    l_sqlBuf := 'select * from dsc_stat.t_stat_zjcqk_r z where rq = '||l_sqlWhereCurrentDay||l_zhlb;
    
    /**
     * 调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
     */
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
    
    l_tableName := F_IDS_GET_TABLENAME('so_sparkZczrzc', I_KHH);
    l_sqlBuf := 'select * from dsc_stat.t_stat_zqzrzc_r z where rq = '||l_sqlWhereCurrentDay||l_zhlb;
    
    /**
     * 调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
     */
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
    
  END; 

---------------------------------------- //1、生成资金余额数据------------------------------------------------  
  BEGIN
    l_tableName := F_IDS_GET_TABLENAME('so_sparkZctjDR', I_KHH);
    l_sqlBuf := "SELECT " ||                                                                                              
                "        KHH,  " ||                                                                                       
                "        cast(0 AS decimal(16,2)) AS zzc_sr       ," ||                                                                          
                "        cast(0 AS decimal(16,2)) AS zzc         ," ||                                                                           
                "        cast(0 AS decimal(16,2)) AS jzc_sr," ||                                            
                "        cast(0 AS decimal(16,2)) AS jzc," ||                                               
                "        cast(0 AS decimal(16,2)) AS zqsz         ," ||                                                                          
                "        cast(0 AS decimal(16,2)) AS zqsz_kt_sr," ||                                                         
                "        cast(0 AS decimal(16,2)) AS zqsz_kt," ||                                                            
                "        cast(0 AS decimal(16,2)) AS cccb_kt_sr," ||                                                         
                "        cast(0 AS decimal(16,2)) AS cccb_kt," ||                                                            
                "        CAST(ROUND(SUM(CASE  " ||                                                    
                "                WHEN BZ = '2' THEN  " ||                                             
                "                    ZHYE * " || l_hlcsHKD ||                                    
                "                WHEN BZ = '3' THEN  " ||                                             
                "                    ZHYE * " || l_hlcsUSD ||                                    
                "                ELSE  " ||                                                           
                "                    ZHYE  " ||                                                       
                "            END),2) AS DECIMAL(16,2)) AS ZJYE,  " ||                                 
                "        cast(0 AS decimal(16,2)) AS ztzc         ," ||                                                      
                "        cast(0 AS decimal(16,2)) AS qtzc         ," ||                                                      
                "        cast(0 AS decimal(16,2)) AS crje         ," ||                                                      
                "        cast(0 AS decimal(16,2)) AS qcje         ," ||                                                      
                "        cast(0 AS decimal(16,2)) AS zrsz         ," ||                                                      
                "        cast(0 AS decimal(16,2)) AS zcsz         ," ||                                                      
                "        cast(0 AS decimal(16,2)) AS zfz         ," ||                                                       
                "        cast(0 AS decimal(16,2)) AS zfz_sr       ," ||                                                      
                "        cast(0 AS decimal(16,2)) AS cccb         ," ||                                                      
                "        cast(0 AS decimal(16,2)) AS dryk         ," ||                                                      
                "        cast(0 AS decimal(16,2)) AS tzzh_fe     ," ||                                                       
                "        cast(0 AS decimal(16,2)) AS tzzh_fe_sr   ," ||                                                               
                "        cast(0 AS decimal(16,2)) AS tzzh_zxfe   ," ||                                                                
                "        cast(0 AS decimal(16,2)) AS tzzh_zxfe_sr ," ||                                                      
                "        cast(0 AS decimal(16,10)) AS tzzh_zxjz   ," ||                                                       
                "        cast(0 AS decimal(16,10)) AS tzzh_zxjz_sr ," ||                     
                "        cast(0 AS decimal(16,8)) AS tzzh_ljjz   ," ||                      
                "        cast(0 AS decimal(16,8)) AS tzzh_ljjz_sr ," ||                     
                "        cast(0 AS decimal(16,2)) AS lxsr         ," ||                     
                "        cast(0 AS decimal(16,2)) AS lxzc     ," ||                         
                "        cast(0 AS decimal(16,2)) AS jyl         " ||                       
                " FROM " || F_IDS_GET_TABLENAME('so_sparkZjye', I_KHH) || "   " ||                             
                " GROUP BY KHH";                                          
                  
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);

  END;
  
 
  ------------------------获取字段----------------------------------------------
  l_tableArr := get_columns(l_tableName);
      l_columns := '( ';
      FOR indx IN l_tableArr.first() .. l_tableArr.last() LOOP
        IF indx = l_tableArr.last() THEN
          l_columns := l_columns || l_tableArr(indx) || ') ';
        ELSE
          l_columns := l_columns || l_tableArr(indx) || ',';
        END IF;
      END LOOP; 
      
      
 ------------------------------------//2、生成证券市值数据----------------------------------------------------
  BEGIN
    l_sqlBuf := "SELECT    " ||
                "        KHH,  "  ||
                "        cast(0 AS decimal(16,2)) AS zzc_sr       ,"  ||
                "        cast(0 AS decimal(16,2)) AS zzc         ,"  ||
                "        cast(0 AS decimal(16,2)) AS jzc_sr,"  ||
                "        cast(0 AS decimal(16,2)) AS jzc,"  ||
                "         CAST(ROUND(SUM(" ||
                "           CASE WHEN CCFX='2' THEN -ZXSZ ELSE ZXSZ END * " ||
                "           CASE WHEN BZ = '2' THEN " || l_hlcsHKD ||
                "                WHEN BZ = '3' THEN  " || l_hlcsUSD ||
                "                ELSE 1 END)" ||
                "        ,2) AS DECIMAL(16,2)) AS ZQSZ,  " ||
                "        cast(0 AS decimal(16,2)) AS zqsz_kt_sr,"  ||
                "        CAST(ROUND(SUM(CASE  WHEN CCFX='2' THEN "  ||
                "                CASE WHEN BZ = '2' THEN  "  ||
                "                    ABS(ZXSZ) * "  || l_hlcsHKD || 
                "                WHEN BZ = '3' THEN  "  ||
                "                    ABS(ZXSZ) * "  || l_hlcsUSD|| 
                "                ELSE  "  ||
                "                    ABS(ZXSZ)  "  ||
                "            END ELSE 0 END),2) AS DECIMAL(16,2)) AS zqsz_kt,  "  ||
                "        cast(0 AS decimal(16,2)) AS cccb_kt_sr,"  ||
                "        CAST(ROUND(SUM(CASE  WHEN CCFX='2' THEN "  ||
                "                CASE WHEN BZ = '2' THEN  "  ||
                "                    CCCB * "  || l_hlcsHKD || 
                "                WHEN BZ = '3' THEN  "  ||
                "                    CCCB * "  || l_hlcsUSD|| 
                "                ELSE  "  ||
                "                    CCCB  "  ||
                "            END ELSE 0 END),2) AS DECIMAL(16,2)) AS cccb_kt,  "  ||
                "        cast(0 AS decimal(16,2)) AS zjye         ,"  ||
                "        cast(0 AS decimal(16,2)) AS ztzc         ,"  ||
                "        cast(0 AS decimal(16,2)) AS qtzc         ,"  ||
                "        cast(0 AS decimal(16,2)) AS crje         ,"  ||
                "        cast(0 AS decimal(16,2)) AS qcje         ,"  ||
                "        cast(0 AS decimal(16,2)) AS zrsz         ,"  ||
                "        cast(0 AS decimal(16,2)) AS zcsz         ,"  ||
                "        cast(0 AS decimal(16,2)) AS zfz         ,"  ||
                "        cast(0 AS decimal(16,2)) AS zfz_sr       ,"  ||
                "        CAST(ROUND(SUM(CASE  "  ||
                "                WHEN BZ = '2' THEN  "  ||
                "                    nvl(CCCB,0) * "  || l_hlcsHKD || 
                "                WHEN BZ = '3' THEN  "  ||
                "                    nvl(CCCB,0) * "  || l_hlcsUSD|| 
                "                ELSE  "  ||
                "                    nvl(CCCB,0)  "  ||
                "            END),2) AS DECIMAL(16,2)) AS CCCB,  "  ||
                "        cast(0 AS decimal(16,2)) AS dryk         ,"  ||
                "        cast(0 AS decimal(16,2)) AS tzzh_fe     ,"  ||
                "        cast(0 AS decimal(16,2)) AS tzzh_fe_sr   ,"  ||
                "        cast(0 AS decimal(16,2)) AS tzzh_zxfe   ,"  ||
                "        cast(0 AS decimal(16,2)) AS tzzh_zxfe_sr ,"  ||
                "        cast(0 AS decimal(16,10)) AS tzzh_zxjz   ,"  ||
                "        cast(0 AS decimal(16,10)) AS tzzh_zxjz_sr ,"  ||
                "        cast(0 AS decimal(16,8)) AS tzzh_ljjz   ,"  ||
                "        cast(0 AS decimal(16,8)) AS tzzh_ljjz_sr ,"  ||
                "        cast(0 AS decimal(16,2)) AS lxsr         ,"  ||
                "        cast(0 AS decimal(16,2)) AS lxzc     ,"  ||
                "        cast(0 AS decimal(16,2)) AS jyl         "   ||             
                "    FROM " || F_IDS_GET_TABLENAME('so_sparkZqyeZctj', I_KHH) || " T" ||
                        "    GROUP BY  KHH"; 

    l_sql := 'INSERT INTO TABLE ' || F_IDS_GET_TABLENAME('so_sparkZctjDR', I_KHH) || l_columns || l_sqlBuf;   
    EXECUTE IMMEDIATE l_sql;

  END;
      
------------------------   //3、存入资金、取出资金数据-----------------------------------------------------------  
  BEGIN
    l_sqlBuf := "SELECT  " ||
                "        KHH," ||
                "        cast(0 AS decimal(16,2)) AS zzc_sr       ," ||
                "        cast(0 AS decimal(16,2)) AS zzc         ," ||
                "        cast(0 AS decimal(16,2)) AS jzc_sr," ||
                "        cast(0 AS decimal(16,2)) AS jzc," ||
                "        cast(0 AS decimal(16,2)) AS zqsz         ," ||
                "        cast(0 AS decimal(16,2)) AS zqsz_kt_sr," ||
                "        cast(0 AS decimal(16,2)) AS zqsz_kt," ||
                "        cast(0 AS decimal(16,2)) AS cccb_kt_sr," ||
                "        cast(0 AS decimal(16,2)) AS cccb_kt," ||
                "        cast(0 AS decimal(16,2)) AS zjye         ," ||
                "        cast(0 AS decimal(16,2)) AS ztzc         ," ||
                "        cast(0 AS decimal(16,2)) AS qtzc         ," ||             
                "       CAST(SUM(CASE WHEN sjkm =  '101'" ||
                "                    THEN fsje * CASE BZ " ||
                "                                WHEN '2' THEN "  || l_hlcsHKD ||
                "                                 WHEN '3' THEN "  || l_hlcsUSD ||
                "                                 ELSE" ||
                "                                 1 " ||
                "                               END" ||
                "                    ELSE 0" ||
                "                END) AS DECIMAL(16,2)) AS CRJE," ||
                "       CAST(SUM(CASE WHEN sjkm = '102'" ||
                "                    THEN fsje * CASE BZ " ||
                "                                WHEN '2' THEN "  || l_hlcsHKD ||
                "                                 WHEN '3' THEN "  || l_hlcsUSD ||
                "                                 ELSE" ||
                "                                 1 " ||
                "                               END" ||
                "                    ELSE 0" ||
                "                END) AS DECIMAL(16,2)) AS QCJE,"  ||               
                "        cast(0 AS decimal(16,2)) AS zrsz         ," ||
                "        cast(0 AS decimal(16,2)) AS zcsz         ," ||
                "        cast(0 AS decimal(16,2)) AS zfz         ," ||
                "        cast(0 AS decimal(16,2)) AS zfz_sr       ," ||
                "        cast(0 AS decimal(16,2)) AS cccb         ," ||
                "        cast(0 AS decimal(16,2)) AS dryk         ," ||
                "        cast(0 AS decimal(16,2)) AS tzzh_fe     ," ||
                "        cast(0 AS decimal(16,2)) AS tzzh_fe_sr   ," ||
                "        cast(0 AS decimal(16,2)) AS tzzh_zxfe   ," ||
                "        cast(0 AS decimal(16,2)) AS tzzh_zxfe_sr ," ||
                "        cast(0 AS decimal(16,10)) AS tzzh_zxjz   ," ||
                "        cast(0 AS decimal(16,10)) AS tzzh_zxjz_sr ," ||
                "        cast(0 AS decimal(16,8)) AS tzzh_ljjz   ," ||
                "        cast(0 AS decimal(16,8)) AS tzzh_ljjz_sr ," ||
                "        CAST(SUM(CASE WHEN SJKM = '105' "  ||
                "                       THEN FSJE * CASE BZ " ||
                "                      WHEN '2' THEN " || l_hlcsHKD ||
                "                      WHEN '3' THEN " || l_hlcsUSD ||
                "                      ELSE 1 " ||
                "                      END " ||
                "                   ELSE 0 " ||
                "                   END) AS DECIMAL(16,2)) AS LXSR," ||
                "        cast(0 AS decimal(16,2)) AS lxzc,"  ||
                "        cast(0 AS decimal(16,2)) AS jyl"    ||
                " FROM "||F_IDS_GET_TABLENAME("so_sparkZjcq", I_KHH)   ||
                " GROUP BY khh";
                
        -- 数据插入至资产统计临时表sparkStatDR
    l_sql := 'INSERT INTO TABLE ' || F_IDS_GET_TABLENAME('so_sparkZctjDR', I_KHH) || l_columns || l_sqlBuf;       
    EXECUTE IMMEDIATE l_sql;
  END; 
 
 --------------------------------市值转入转出----------------------------------------
  BEGIN
    l_sqlBuf := "SELECT " ||
                "        T.KHH,  " ||
                "        0 AS zzc_sr       ," ||
                "        0 AS zzc         ," ||
                "        0 AS jzc_sr," ||
                "        0 AS jzc," ||
                "        0 AS ZQSZ,  " ||
                "        0 AS zqsz_kt_sr," ||
                "        0 AS zqsz_kt,  " ||
                "        0 AS cccb_kt_sr," ||
                "        0 AS cccb_kt,  " ||
                "        0 AS zjye         ," ||
                "        0 AS ztzc         ," ||
                "        0 AS qtzc         ," ||
                "        0 AS crje,        " ||
                "        0 AS qcje,        " ||
                "       CAST(SUM(CASE WHEN cqfx = '1'" ||
                "                    THEN zqsz * CASE BZ " ||
                "                                WHEN '2' THEN " || l_hlcsHKD ||
                "                                WHEN '3' THEN " || l_hlcsUSD ||
                "                                ELSE" ||
                "                                 1 " ||
                "                               END" ||
                "                    ELSE 0" ||
                "                END) AS DECIMAL(16,2)) AS ZRSZ," ||
                "       CAST(SUM(CASE WHEN cqfx = '2'" ||
                "                    THEN zqsz * CASE BZ " ||
                "                                WHEN '2' THEN " || l_hlcsHKD ||
                "                                WHEN '3' THEN " || l_hlcsUSD ||
                "                                ELSE" ||
                "                                 1 " ||
                "                               END" ||
                "                    ELSE 0" ||
                "                END) AS DECIMAL(16,2)) AS ZCSZ," ||
                "        0 AS zfz         ," ||
                "        0 AS zfz_sr       ," ||
                "        0 AS CCCB,  " ||
                "        0 AS dryk         ," ||
                "        0 AS tzzh_fe     ," ||
                "        0 AS tzzh_fe_sr   ," ||
                "        0 AS tzzh_zxfe   ," ||
                "        0 AS tzzh_zxfe_sr ," ||
                "        0 AS tzzh_zxjz   ," ||
                "        0 AS tzzh_zxjz_sr ," ||
                "        0 AS tzzh_ljjz   ," ||
                "        0 AS tzzh_ljjz_sr ," ||
                "        0 AS lxsr         ," ||
                "        0 AS lxzc     ," ||
                "        0 AS jyl" ||
                " FROM "||F_IDS_GET_TABLENAME("so_sparkZczrzc", I_KHH) ||
                " GROUP BY khh";
  END;
  
  ---------------------------------------- //4、上日数据------------------------------------------------
  BEGIN
    l_sqlBuf := "SELECT   " ||
                "        KHH," ||
                "        ZZC       AS ZZC_SR," ||
                "        cast(0 AS decimal(16,2)) AS zzc         ," ||
                "        jzc AS jzc_sr," ||
                "        cast(0 AS decimal(16,2)) AS jzc," ||
                "        cast(0 AS decimal(16,2)) AS zqsz         ," ||
                "        zqsz_kt AS zqsz_kt_sr," ||
                "        cast(0 AS decimal(16,2)) AS zqsz_kt," ||
                "        cccb_kt AS cccb_kt_sr," ||
                "        cast(0 AS decimal(16,2)) AS cccb_kt," ||
                "        cast(0 AS decimal(16,2)) AS zjye         ," ||
                "        cast(0 AS decimal(16,2)) AS ztzc         ," ||
                "        cast(0 AS decimal(16,2)) AS qtzc         ," ||
                "        cast(0 AS decimal(16,2)) AS crje         ," ||
                "        cast(0 AS decimal(16,2)) AS qcje         ," ||
                "        cast(0 AS decimal(16,2)) AS zrsz         ," ||
                "        cast(0 AS decimal(16,2)) AS zcsz         ," ||
                "        cast(0 AS decimal(16,2)) AS zfz          ," ||
                "        ZFZ       AS ZFZ_SR," ||
                "        cast(0 AS decimal(16,2)) AS cccb         ," ||
                "        cast(0 AS decimal(16,2)) AS dryk         ," ||
                "        cast(0 AS decimal(16,2)) AS tzzh_fe      ," ||
                "        TZZH_FE   AS TZZH_FE_SR ," ||
                "        cast(0 AS decimal(16,2)) AS tzzh_zxfe    ," ||
                "        TZZH_ZXFE AS TZZH_ZXFE_SR," ||
                "        cast(0 AS decimal(16,10)) AS tzzh_zxjz   ," ||
                "        TZZH_ZXJZ AS TZZH_ZXJZ_SR," ||
                "        cast(0 AS decimal(16,8)) AS tzzh_ljjz    ," ||
                "        TZZH_LJJZ AS TZZH_LJJZ_SR," ||
                "        cast(0 AS decimal(16,2)) AS lxsr         ," ||
                "        cast(0 AS decimal(16,2)) AS lxzc     ," ||
                "        cast(0 AS decimal(16,2)) AS jyl         " ||
                "FROM "|| F_IDS_GET_TABLENAME('so_sparkZcSr', I_KHH) ||" T    ";
    
    
        -- 数据插入至资产统计临时表sparkStatDR
    l_sql := 'INSERT INTO TABLE ' || F_IDS_GET_TABLENAME('so_sparkZctjDR', I_KHH) || l_columns || l_sqlBuf;       
    EXECUTE IMMEDIATE l_sql;
  END;   
  
---------------------------------------- //5、生成当日统计最终数据------------------------------------------------
 
  BEGIN
    l_tableName := F_IDS_GET_TABLENAME('so_sparkZctjResult', I_KHH);
    l_sqlBuf := "SELECT   " ||
             "      KHH,   " ||
             "      CAST(ROUND((CASE WHEN ZZC_SR = 0 AND ZZC > 0 THEN ZZC - DRYK ELSE ZZC_SR END),2) AS DECIMAL(16,2)) AS ZZC_SR,   " ||
             "      CAST(ROUND(ZZC,2) AS DECIMAL(16,2)) AS ZZC,   " ||
             "      CAST(ROUND(JZC_SR,2) AS DECIMAL(16,2)) AS JZC_SR,   " ||
             "      CAST(ROUND(JZC,2) AS DECIMAL(16,2)) AS JZC,   " ||
             "      CAST(ROUND(ZQSZ,2) AS DECIMAL(16,2)) AS ZQSZ,   " ||
             "      CAST(ROUND(ZQSZ_KT_SR,2) AS DECIMAL(16,2)) AS ZQSZ_KT_SR,   " ||
             "      CAST(ROUND(ZQSZ_KT,2) AS DECIMAL(16,2)) AS ZQSZ_KT,   " ||
             "      CAST(ROUND(CCCB_KT_SR,2) AS DECIMAL(16,2)) AS CCCB_KT_SR,   " ||
             "      CAST(ROUND(CCCB_KT,2) AS DECIMAL(16,2)) AS CCCB_KT,   " ||
             "      CAST(ROUND(ZJYE,2) AS DECIMAL(16,2)) AS ZJYE,   " ||
             "      CAST(ROUND(ZTZC,2) AS DECIMAL(16,2)) AS ZTZC,   " ||
             "      CAST(ROUND(QTZC,2) AS DECIMAL(16,2)) AS QTZC,   " ||
             "      CAST(ROUND(CRJE,2) AS DECIMAL(16,2)) AS CRJE,   " ||
             "      CAST(ROUND(QCJE,2) AS DECIMAL(16,2)) AS QCJE,   " ||
             "      CAST(ROUND(ZRSZ,2) AS DECIMAL(16,2)) AS ZRSZ,   " ||
             "      CAST(ROUND(ZCSZ,2) AS DECIMAL(16,2)) AS ZCSZ,   " ||
             "      CAST(ROUND(ZFZ,2) AS DECIMAL(16,2)) AS ZFZ,   " ||
             "      CAST(ROUND(ZFZ_SR,2) AS DECIMAL(16,2)) AS ZFZ_SR,   " ||
             "      CAST(ROUND(CCCB,2) AS DECIMAL(16,2)) AS CCCB,   " ||
             "      CAST(ROUND(DRYK,2) AS DECIMAL(16,2)) AS DRYK,   " ||
             "      CAST(ROUND((CASE WHEN TZZH_FE = 0 THEN 0 WHEN ZZC_SR = 0 AND ZZC > 0 THEN ZZC - ZFZ - DRYK WHEN ZZC_SR > 0 AND ZZC = 0 THEN 0 ELSE TZZH_FE END),2) AS DECIMAL(16,2)) AS TZZH_FE,  " ||
             "      CAST(ROUND((CASE WHEN ZZC_SR = 0 AND ZZC > 0 THEN ZZC - ZFZ - DRYK ELSE TZZH_FE_SR END),2) AS DECIMAL(16,2)) AS TZZH_FE_SR,  " ||
             "      CAST(ROUND((CASE WHEN TZZH_ZXFE = 0 THEN 0 WHEN ZZC_SR = 0 AND ZZC > 0 THEN ZZC - ZFZ - DRYK WHEN ZZC_SR > 0 AND ZZC = 0 THEN 0 ELSE TZZH_ZXFE END),2) AS DECIMAL(16,2)) AS TZZH_ZXFE,  " ||
             "      CAST(ROUND((CASE WHEN ZZC_SR = 0 AND ZZC > 0 THEN ZZC - ZFZ - DRYK ELSE TZZH_ZXFE_SR END),2) AS DECIMAL(16,2)) AS TZZH_ZXFE_SR,  " ||
             "      CAST(ROUND((CASE WHEN TZZH_ZXFE = 0 THEN 1 WHEN ZZC_SR = 0 AND ZZC > 0 THEN ROUND(IF((ZZC-DRYK-ZFZ) = 0, 0, (ZZC-ZFZ) / (ZZC-ZFZ-DRYK)), 10) WHEN ZZC_SR > 0 AND ZZC = 0 THEN 1 ELSE TZZH_ZXJZ END),4) AS DECIMAL(10,4)) AS TZZH_ZXJZ,  " ||
             "      CAST(ROUND((CASE WHEN ZZC_SR = 0 AND ZZC > 0 THEN 1.0 ELSE TZZH_ZXJZ_SR END),4) AS DECIMAL(10,4)) AS TZZH_ZXJZ_SR,  " ||
             "      CAST(ROUND((CASE WHEN TZZH_FE = 0 THEN 1 WHEN ZZC_SR = 0 AND ZZC > 0 THEN ROUND(IF((ZZC-DRYK-ZFZ) = 0, 0, (ZZC-ZFZ) / (ZZC-ZFZ-DRYK)), 10) WHEN ZZC_SR > 0 AND ZZC = 0 THEN 1 ELSE TZZH_LJJZ END),4) AS DECIMAL(22,4)) AS TZZH_LJJZ,  " ||
             "      CAST(ROUND((CASE WHEN ZZC_SR = 0 AND ZZC > 0 THEN 1.0 ELSE TZZH_LJJZ_SR END),4) AS DECIMAL(22,4)) AS TZZH_LJJZ_SR,  " ||
             "      CAST(ROUND(LXSR,2) AS DECIMAL(16,2)) AS LXSR,   " ||
             "      CAST(ROUND(LXZC,2) AS DECIMAL(16,2)) AS LXZC,   " ||
             "      CAST(ROUND(JYL,2) AS DECIMAL(16,2)) AS JYL,   " ||
             I_RQ || " as RQ" ||
             "   FROM   " ||
             "   (SELECT  " ||
             "      T.KHH,   " ||
             "      NVL(ZZC_SR, 0) AS ZZC_SR,   " ||
             "      NVL(ZZC, 0) AS ZZC,   " ||
             "      NVL(JZC_SR, 0) AS JZC_SR,   " ||
             "      NVL(JZC, 0) AS JZC,   " ||
             "      NVL(ZQSZ, 0) AS ZQSZ,   " ||
             "      NVL(ZQSZ_KT_SR, 0) AS ZQSZ_KT_SR,   " ||
             "      NVL(ZQSZ_KT, 0) AS ZQSZ_KT,   " ||
             "      NVL(CCCB_KT_SR, 0) AS CCCB_KT_SR,   " ||
             "      NVL(CCCB_KT, 0) AS CCCB_KT,   " ||
             "      NVL(ZJYE, 0) AS ZJYE,   " ||
             "      NVL(ZTZC, 0) AS ZTZC,   " ||
             "      NVL(QTZC, 0) AS QTZC,   " ||
             "      NVL(CRJE, 0) AS CRJE,   " ||
             "      NVL(QCJE, 0) AS QCJE,   " ||
             "      NVL(ZRSZ, 0) AS ZRSZ,   " ||
             "      NVL(ZCSZ, 0) AS ZCSZ,   " ||
             "      NVL(ZFZ_SR, 0) AS ZFZ_SR,   " ||
             "      NVL(ZFZ, 0) AS ZFZ,   " ||
             "      NVL(CCCB, 0) AS CCCB,   " ||
             "      NVL((ZZC - ZFZ) - (ZZC_SR - ZFZ_SR) - (CRJE - QCJE) - (ZRSZ - ZCSZ), 0) AS DRYK,   " ||
             "      ROUND(NVL(TZZH_FE,0),2) AS TZZH_FE,   " ||
             "      NVL(TZZH_FE_SR, 0) AS TZZH_FE_SR,   " ||
             "      NVL(TZZH_ZXFE,0) AS TZZH_ZXFE,   " ||
             "      NVL(TZZH_ZXFE_SR, 0) AS TZZH_ZXFE_SR,    " ||
             "      IF(NVL(TZZH_ZXFE,0) = 0, 1.0, ROUND((NVL(ZZC,0)-NVL(ZFZ,0)) / TZZH_ZXFE,4)) AS TZZH_ZXJZ,   " ||
             "      NVL(TZZH_ZXJZ_SR, 0) AS TZZH_ZXJZ_SR,   " ||
             "      IF(NVL(TZZH_FE,0) = 0, 1.0, ROUND((NVL(ZZC,0)-NVL(ZFZ,0)) / TZZH_FE,4)) AS TZZH_LJJZ,   " ||
             "      NVL(TZZH_LJJZ_SR, 0) AS TZZH_LJJZ_SR,   " ||
             "      NVL(LXSR, 0) AS LXSR,   " ||
             "      NVL(LXZC, 0) AS LXZC,   " ||
             "      NVL(JYL, 0) AS JYL  " ||
             "   FROM   " ||
             "    (SELECT  " ||
             "          T.KHH,   " ||
             "          NVL(SUM(ZZC_SR), 0) AS ZZC_SR,   " ||
             "          NVL(SUM(ZQSZ), 0) + NVL(SUM(ZJYE), 0) + NVL(SUM(ZTZC), 0) +  NVL(SUM(QTZC), 0) AS ZZC,   " ||
             "          NVL(SUM(JZC_SR), 0) AS JZC_SR,   " ||
             "          NVL(SUM(ZQSZ), 0) + NVL(SUM(ZJYE), 0) + NVL(SUM(ZTZC), 0) + NVL(SUM(QTZC), 0)  AS JZC,   " ||
             "          NVL(SUM(ZQSZ), 0) AS ZQSZ,   " ||
             "          NVL(SUM(ZQSZ_KT_SR), 0) AS ZQSZ_KT_SR,   " ||
             "          NVL(SUM(ZQSZ_KT), 0) AS ZQSZ_KT,   " ||
             "          NVL(SUM(CCCB_KT_SR), 0) AS CCCB_KT_SR,   " ||
             "          NVL(SUM(CCCB_KT), 0) AS CCCB_KT,   " ||
             "          NVL(SUM(ZJYE), 0) AS ZJYE,   " ||
             "          NVL(SUM(ZTZC), 0) AS ZTZC,   " ||
             "          NVL(SUM(QTZC), 0) AS QTZC,   " ||
             "          NVL(SUM(CRJE), 0) AS CRJE,   " ||
             "          NVL(SUM(QCJE), 0) AS QCJE,   " ||
             "          NVL(SUM(ZRSZ), 0) AS ZRSZ,   " ||
             "          NVL(SUM(ZCSZ), 0) AS ZCSZ,   " ||
             "          NVL(SUM(ZFZ_SR), 0) AS ZFZ_SR,   " ||
             "          NVL(SUM(ZFZ), 0) AS ZFZ,   " ||
             "          NVL(SUM(CCCB), 0) AS CCCB,   " ||
             "          CASE    " ||
             "                  WHEN NVL(SUM(TZZH_LJJZ_SR), 1.0)=0 THEN   " ||
             --如果上日累计净值为0，取本日资产作为份额
             "            NVL(SUM(ZQSZ), 0) + NVL(SUM(ZJYE), 0) +   " ||
             "                  NVL(SUM(ZTZC), 0) + NVL(SUM(QTZC), 0) - NVL(SUM(ZFZ),0)  " ||
             "          ELSE    " ||
             "            IF(SUM(TZZH_ZXFE_SR) is NULL,  " ||
             "                      NVL(SUM(ZQSZ), 0) + NVL(SUM(ZJYE), 0) +   " ||
             "                      NVL(SUM(ZTZC), 0) + NVL(SUM(QTZC), 0) - NVL(SUM(ZFZ),0) ,   " ||
             "                      NVL(SUM(TZZH_FE_SR),0) +   " ||
             "                      (NVL(SUM(CRJE),0) + NVL(SUM(LXSR),0) - NVL(SUM(QCJE),0) + NVL(SUM(ZRSZ),0) - NVL(SUM(ZCSZ),0)) / NVL(SUM(TZZH_LJJZ_SR), 1.0))   " ||
             "        END AS TZZH_FE,   " ||
             "          NVL(SUM(TZZH_FE_SR), 0) AS TZZH_FE_SR,   " ||
             "          NVL(SUM(TZZH_LJJZ_SR), 0) AS TZZH_LJJZ_SR,   " ||
             "        CASE    " ||
             "                  WHEN NVL(SUM(TZZH_ZXJZ_SR), 1.0)=0 THEN   " ||
             --如果上日累计净值为0，取本日资产作为最新份额
             "            NVL(SUM(ZQSZ), 0) + NVL(SUM(ZJYE), 0) +   " ||
             "                  NVL(SUM(ZTZC), 0) + NVL(SUM(QTZC), 0)- NVL(SUM(ZFZ),0) " ||
             "          ELSE   " ||
             "            IF(SUM(TZZH_ZXFE_SR) is NULL,   " ||
             "                      NVL(SUM(ZQSZ), 0) + NVL(SUM(ZJYE), 0) +   " ||
             "                      NVL(SUM(ZTZC), 0) + NVL(SUM(QTZC), 0) - NVL(SUM(ZFZ),0), " ||
             "                      NVL(SUM(TZZH_ZXFE_SR),0) +   " ||
             "                      (NVL(SUM(CRJE),0) + NVL(SUM(LXSR),0) - NVL(SUM(QCJE),0) + NVL(SUM(ZRSZ),0) - NVL(SUM(ZCSZ),0)) / NVL(SUM(TZZH_ZXJZ_SR), 1.0))   " ||
             "        END AS TZZH_ZXFE,   " ||
             "          NVL(SUM(TZZH_ZXFE_SR), 0) AS TZZH_ZXFE_SR,   " ||
             "          NVL(SUM(TZZH_ZXJZ_SR), 0) AS TZZH_ZXJZ_SR,   " ||
             "          NVL(SUM(LXSR), 0) AS LXSR,   " ||
             "        NVL(SUM(LXZC), 0) AS LXZC,   " ||
             "        NVL(SUM(JYL), 0) AS JYL " ||
             "    FROM "|| F_IDS_GET_TABLENAME('so_sparkZctjDR', I_KHH) ||" T  " ||
             "    GROUP BY  KHH) T LEFT JOIN CUST.T_KHXX_JJYW A   " ||
             "   ON A.KHH = T.KHH WHERE NOT(A.XHRQ IS NOT NULL AND A.XHRQ < " || I_RQ || "  ))A";
    
     F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
  
  END;  

  BEGIN
    /**
     * 写入分区表
     * 入参：临时表名，目标库名，目标表名，目标表分区字段值，客户号
     * F_IDS_OVERWRITE_PARTITION(tablename, dbname,targetTable,partitionValue, khh)
     */ 
    F_IDS_OVERWRITE_PARTITION(l_tableName, "CUST", "t_stat_so_zc_r", I_RQ, I_KHH);
  END; 
  
end;
/