!set plsqlUseSlash true
CREATE OR REPLACE PROCEDURE CUST.P_IDS_INIT_CALC_ZSJZ_NJZQ(
I_RQ IN INT)
IS
/******************************************************************
  *文件名称：CUST.P_IDS_INIT_CALC
  *项目名称：IDS计算
  *文件说明：集中交易清算初始化

  创建人：燕居庆
  功能说明：集中交易清算初始化，清算第一天将柜台数据全部写入，以该天作为计算基准值

  参数说明

  修改者        版本号        修改日期        说明
  燕居庆        v1.0.0        2019/6/14       创建
  燕居庆        v1.0.1        2019/9/24       针对南证现金宝A72001特殊处理：cpsl=0也计入金融产品持仓中
  
  -----------------------------------------------------------------------------
  邱建锋  v2.0.1    2021/1/5    调整净值计算方式，新增净值折算，表结构等有所调整
  钟梦涵        v2.0.2        2021/11/04      调整客户号的过滤条件
*******************************************************************/
l_sqlBuf STRING; --创建表语句
l_tableName STRING; --临时表名
l_sqlWhereCurrentDay STRING; 
l_hlcsHKD DECIMAL(12,6);
l_hlcsUSD DECIMAL(12,6);
BEGIN
  l_sqlWhereCurrentDay := I_RQ;
  
  --获取柜台证券余额
  BEGIN
        l_tableName := F_IDS_GET_TABLENAME('sparkZqyeGt', NULL);
        l_sqlBuf := "select 0 tzzh_id,rq,khh,jys,gdh,zqdm,zqlb,bz,zqsl,null as kcrq,null as fltsl,zxsz,"  ||
                    " null as cccb,null as cbj,null as ljyk,null as tbcccb,null as tbcbj,null as dryk, ZQSL AS ZQSL_ORIG"  || 
                    " from dsc_bas.t_zqye_his t where zqdm <> '888880' and t.zqlb <> 'F8' and t.zqsl>0 AND T.ZQDM NOT LIKE 'SHRQ%' AND T.ZQDM NOT LIKE 'SZRQ%'  and rq = " || l_sqlWhereCurrentDay;-----T日
        
        F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
  END;
  
  --获取持仓数据
  BEGIN
    l_tableName := F_IDS_GET_TABLENAME('sparkZqyeGT_0', NULL);
    l_sqlBuf := "select 0 as TZZH_ID,  " ||
                "     KHH,  " ||
                "     JYS,  " ||
                "     GDH,  " ||
                "     ZQDM,  " ||
                I_RQ || " as kcrq,   " ||
                "     ZQLB,  " ||
                "     BZ,  " ||
                "     ZQSL,  " ||
                "     FLTSL,  " ||
                "      ZXSZ,  " ||
                I_RQ || " as rq " ||
                " FROM " || F_IDS_GET_TABLENAME('sparkZqyeGt', NULL);
    
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
  END;
  
  -- 将持仓数据合并后写入到cust.t_zqye_his对应分区
  BEGIN
    l_tableName := F_IDS_GET_TABLENAME('sparkZqyeGTResult', NULL);
    l_sqlBuf := 'select ' ||
                '   0 as tzzh_id, ' ||
                '   khh,' ||
                '   jys,' ||
                '   gdh,' ||
                '   zqdm,' ||
                '   max(kcrq) as kcrq,' ||
                '   min(zqlb) as zqlb,' ||
                '   max(bz) as bz,' ||
                '   sum(zqsl) as zqsl,' ||
                '   sum(fltsl) as fltsl,' ||
                '   sum(zxsz) as zxsz,' ||
                '   sum(zxsz) as cccb,' ||
                '   sum(zxsz)/sum(zqsl) as cbj,' ||
                '   0 as ljyk,' ||
                '   sum(zxsz) as tbcccb,' ||
                '   sum(zxsz)/sum(zqsl) as tbcbj,' ||
                '   0 as dryk,' ||
                '   sum(zqsl) as zqsl_orig,' ||
                '   rq' ||
                ' from ' || F_IDS_GET_TABLENAME('sparkZqyeGT_0', NULL) || ' group by khh,jys,gdh,zqdm,rq';
    
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
    F_IDS_OVERWRITE_PARTITION(l_tableName, 'cust', 'T_ZQYE_HIS', I_RQ, NULL);
  END;
  
  --成本统计
  BEGIN
    l_tableName := F_IDS_GET_TABLENAME('sparkZqyeCbjsGT', NULL);
    l_sqlBuf := 'SELECT TZZH_ID, KHH, JYS, GDH, ZQDM, KCRQ, ZQLB, BZ, ZQSL,ZXSZ AS ZQSZ,IF(CCCB<>0,CCCB,ZXSZ) AS CCCB,T.ljyk AS LJYK,' ||
                '   ZQSL AS DRMRSL,ZQSL AS LJMRSL,ZQSL AS DRYSSL,ZXSZ AS DRMRJE,ZXSZ AS LJMRJE, RQ' ||
                '              FROM CUST.T_ZQYE_HIS T' ||
                '             WHERE RQ = ' || I_RQ ;
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
    F_IDS_OVERWRITE_PARTITION(l_tableName, 'cust', 't_zqye_cbjs', I_RQ, NULL);
  END;
  
  --场外开基持仓
  BEGIN
    l_tableName := F_IDS_GET_TABLENAME('sparkFPcpfeGT', NULL);
    l_sqlBuf := "SELECT KHH,JRCPZH,JRJGDM,CPDM, CPJC,cast('1' AS STRING) AS SFFS,SUM(CPSL) AS CPSL,BZ,SUM(ZXSZ) AS ZXSZ,CPFL,KCRQ,SUM(ZXSZ) AS CCCB,0 AS LJYK,SUM(ZXSZ) AS TBCCCB," ||
                " CAST(SUM(ZXSZ)/SUM(CPSL) AS DECIMAL(9,4)) AS TBCBJ,0 AS DRYK,CAST(SUM(ZXSZ)/SUM(CPSL) AS DECIMAL(9,4)) AS CBJ," ||
                " app_id, RQ FROM" ||
                "(SELECT KHH,NVL(jrcpzh,'0') AS JRCPZH,JRJGDM,CPDM,CPJC,SFFS,CPSL,BZ,ZXSZ,CPFL," || 
                I_RQ || " AS KCRQ,ZXSZ AS CCCB,0 AS LJYK, ZXSZ AS TBCCCB,CAST(ZXSZ/CPSL AS DECIMAL(9,4)) AS TBCBJ," ||
                "   0 AS DRYK,CAST(ZXSZ/CPSL AS DECIMAL(9,4)) AS CBJ,app_id," || I_RQ || " AS RQ" ||
                "   FROM DSC_BAS.T_FP_CPFE_HIS WHERE RQ = " || I_RQ || " AND (CPSL!=0 OR CPDM = 'A72001')) T" ||
                " GROUP BY KHH,app_id,JRCPZH,JRJGDM,CPDM,BZ,KCRQ,RQ,CPFL,CPJC"; --将CPJC加入groupby中
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
    F_IDS_OVERWRITE_PARTITION(l_tableName, 'cust', 't_fp_cpfe_his', I_RQ, NULL);
  END;
  
  --场外开基成本
  BEGIN
    l_tableName := F_IDS_GET_TABLENAME('sparkFPcpfeCbjsGT', NULL);
    l_sqlBuf := "SELECT 0 AS TZZH_ID,KHH,JRCPZH,JRJGDM,CPDM,CPFL,BZ,KCRQ,CPSL,ZXSZ AS ZQSZ," ||
                "   CCCB,LJYK,CPSL AS DRMRSL,CPSL AS LJMRSL,CCCB AS DRMRJE,CCCB AS LJMRJE,app_id," || I_RQ || " AS RQ" ||
                "   FROM CUST.T_FP_CPFE_HIS WHERE RQ=" || I_RQ ;
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
    F_IDS_OVERWRITE_PARTITION(l_tableName, 'cust', 't_fp_cpfe_cbjs', I_RQ, NULL);
  END;
  
  --将数据内容合并，生成当日资产数据
  BEGIN
    -- 获取汇率参数
    SELECT CAST(F_GET_HLCS('2',I_RQ) AS DECIMAL(12,6)) INTO l_hlcsHKD FROM `SYSTEM`.dual;
    SELECT CAST(F_GET_HLCS('3',I_RQ) AS DECIMAL(12,6)) INTO l_hlcsUSD FROM `SYSTEM`.dual;
  END;
  
  BEGIN
    --创建临时表
    l_tableName := F_IDS_GET_TABLENAME('sparkStatDR', NULL);
    l_sqlBuf := "SELECT * FROM CUST.T_STAT_ZC_R WHERE 1=2";
    
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
  END;
  
  BEGIN
    --生成资金余额数据写入临时表
    l_sqlBuf := "INSERT INTO " || F_IDS_GET_TABLENAME('sparkStatDR', NULL) ||
                " (            " ||
                " TZZH_ID,     " ||
                " KHH,         " ||
                " ZJYE)        " || 
                " SELECT       " || 
                " TZZH_ID,     " ||
                " KHH,         " ||
                " CAST(SUM(CASE WHEN BZ = '2' THEN ZHYE *  " || l_hlcsHKD ||
                "          WHEN BZ = '3' THEN ZHYE * " || l_hlcsUSD ||
                "          ELSE ZHYE  " || 
                "     END) AS DECIMAL(16,2)) AS ZJYE " ||
                " FROM cust.t_zjye_his T WHERE rq = " || I_RQ ||
                " GROUP BY TZZH_ID, KHH ";
                
    EXECUTE IMMEDIATE l_sqlBuf;
  END;
  
  BEGIN
    --生成证券市值导入临时表
    l_sqlBuf := "INSERT INTO TABLE " || F_IDS_GET_TABLENAME('sparkStatDR', NULL) || 
                " (
                  TZZH_ID,
                  KHH,
                  ZQSZ,
                  CCCB) 
                SELECT 
                  TZZH_ID,
                  KHH,
                  CAST(SUM(CASE WHEN T.BZ = '2' THEN 
                            ZXSZ *  " || l_hlcsHKD ||
                "          WHEN T.BZ = '3' THEN 
                            ZXSZ *  " || l_hlcsUSD ||
                "          ELSE 
                             ZXSZ 
                      END) AS DECIMAL(16,2)) AS ZQSZ,
                  CAST(SUM(CASE WHEN T.BZ = '2' THEN
                      NVL(CCCB,0) * " || l_hlcsHKD ||
                "   WHEN T.BZ = '3' THEN
                      NVL(CCCB, 0) * " || l_hlcsUSD ||
                "   ELSE
                      NVL(CCCB, 0)
                   END) AS DECIMAL(16,2)) AS CCCB " ||
                " FROM (SELECT * FROM CUST.T_ZQYE_HIS T WHERE RQ = " || I_RQ ||" ) T " ||
                " LEFT SEMI JOIN (SELECT * FROM DSC_CFG.VW_T_ZQLB_IDS WHERE IS_JSSZ = 1)  D ON (T.ZQLB =D.ZQLB) " ||
                " GROUP BY TZZH_ID, KHH";
                
    EXECUTE IMMEDIATE l_sqlBuf;
  END;
  
  BEGIN
    --非流通证券市值导入
    l_sqlBuf := "INSERT INTO TABLE  " || F_IDS_GET_TABLENAME('sparkStatDR', NULL) || 
                " (
                  TZZH_ID,
                  KHH,
                  ZQSZ_FLT
                  ) 
                SELECT 
                  0 AS TZZH_ID,
                  KHH,
                  CAST(SUM(CASE WHEN BZ = '2' THEN 
                            ZXSZ *  " || l_hlcsHKD || 
                "           WHEN BZ = '3' THEN 
                            ZXSZ *  " || l_hlcsUSD ||
                "           ELSE 
                             ZXSZ 
                      END) AS DECIMAL(16,2)) AS ZQSZ_FLT
                FROM DSC_BAS.T_ZQYE_FLT_HIS T WHERE rq = " || I_RQ ||
                " GROUP BY KHH";
                
    EXECUTE IMMEDIATE l_sqlBuf;
  END;
  
  BEGIN
    --金融产品数据导入
    l_sqlBuf := "INSERT INTO TABLE " || F_IDS_GET_TABLENAME('sparkStatDR', NULL) || 
                " (
                  TZZH_ID,
                  KHH,
                  ZQSZ,
                  CCCB,
                  ZQSZ_JRCP) 
                SELECT 
                  0 AS TZZH_ID,
                  KHH,
                  CAST(SUM(CASE WHEN BZ = '2' THEN 
                            ZXSZ *  " || l_hlcsHKD || 
                "          WHEN BZ = '3' THEN 
                           ZXSZ *  " || l_hlcsUSD  ||
                "         ELSE 
                             ZXSZ 
                      END) AS DECIMAL(16,2)) AS ZQSZ,
                CAST(SUM(CASE WHEN BZ = '2' THEN 
                            NVL(CCCB,0) *  " || l_hlcsHKD || 
                "          WHEN BZ = '3' THEN 
                           NVL(CCCB,0) *  " || l_hlcsUSD  ||
                "         ELSE 
                             NVL(CCCB,0) 
                      END) AS DECIMAL(16,2)) AS CCCB,
                  CAST(SUM(CASE WHEN BZ = '2' THEN 
                            ZXSZ *  " || l_hlcsHKD ||
                "          WHEN BZ = '3' THEN 
                            ZXSZ *  " || l_hlcsUSD ||
                "          ELSE 
                             ZXSZ 
                      END) AS DECIMAL(16,2)) AS ZQSZ_JRCP
                FROM CUST.T_FP_CPFE_HIS T WHERE RQ = " || I_RQ ||
                " GROUP BY  KHH ";
    EXECUTE IMMEDIATE l_sqlBuf;
  END;
    -- 在途资产等修正数据
    -- 在途资产(主要指交收规则引起的未入资产)
    -- 1、4：抵押品市值（包括：质押入库债券市值,  股票质押回购市值）
    -- 3： 债权资产(包括：质押式回购融券，报价回购融资，转融通出借)
    -- 31、32、33、34：市值修正金额(主要包括分红扩股、配股的修正)
    -- 总负债：这里取值为 质押式回购融资，股票质押回购融资金额
  BEGIN
    l_sqlBuf := "INSERT INTO TABLE " || F_IDS_GET_TABLENAME('sparkStatDR', NULL) || 
                " (TZZH_ID,
                  KHH,
                  ZQSZ_DYP,
                  ZTZC,
                  QTZC,
                  ZFZ)
                SELECT
                  0 as TZZH_ID,
                  KHH,
                  CAST(SUM(CASE
                    WHEN XZLB IN ('4') THEN
                      XZJE * CASE BZ
                        WHEN '2' THEN
                          " || l_hlcsHKD || "
                        WHEN '3' THEN 
                          " || l_hlcsUSD || "
                        ELSE 
                          1
                        END
                    ELSE
                      0
                    END) AS DECIMAL(16,2)) AS ZQSZ_DYP,
                  CAST(SUM(CASE
                    WHEN XZLB IN ('11', '12', '13', '14', '15', '22', '23', '20', '21') THEN
                      XZJE * CASE BZ
                        WHEN '2' THEN
                          " || l_hlcsHKD || "
                        WHEN '3' THEN 
                          " || l_hlcsUSD || "
                        ELSE 
                          1
                        END
                    ELSE
                      0
                    END) AS DECIMAL(16,2)) AS ZTZC,
                  CAST(SUM(CASE 
                    WHEN XZLB IN ('6', '7', '3', '31', '32', '33', '34') THEN 
                      XZJE * CASE BZ
                        WHEN '2' THEN
                          " || l_hlcsHKD || "
                        WHEN '3' THEN 
                          " || l_hlcsUSD || "
                        ELSE 
                          1
                        END
                    ELSE
                      0
                    END) AS DECIMAL(16,2)) AS QTZC,
                  CAST(SUM(CASE
                    WHEN XZLB IN ('2', '5') THEN
                       XZJE * CASE BZ
                        WHEN '2' THEN
                          " || l_hlcsHKD || "
                        WHEN '3' THEN 
                          " || l_hlcsUSD || "
                        ELSE 
                          1
                        END
                    ELSE
                      0
                    END) AS DECIMAL(16,2)) AS ZFZ
                  FROM DSC_STAT.T_STAT_KHZCXZMX T WHERE " || I_RQ || " BETWEEN KSRQ AND JSRQ
                  AND ZHLB IN ('1', '3')
                  GROUP BY KHH";  
                  
    EXECUTE IMMEDIATE l_sqlBuf;
  END;
    -- 合并生成最终数据
    -- 如果上日累计净值为0，取本日资产作为份额
  begin
    l_tableName := F_IDS_GET_TABLENAME('sparkStatDRResult', NULL);
    l_sqlBuf := "SELECT TZZH_ID," ||
                "         KHH," ||
                "         CAST((CASE" ||
                "                WHEN ZZC_SR = 0 AND ZZC > 0 THEN" ||
                "                 ZZC - DRYK" ||
                "                ELSE" ||
                "                 ZZC_SR" ||
                "              END) AS DECIMAL(16, 2)) AS ZZC_SR," ||
                "         ZZC," ||
                "         ZQSZ," ||
                "         ZQSZ_FLT," ||
                "         ZQSZ_DYP," ||
                "         ZJYE," ||
                "         ZTZC," ||
                "         QTZC," ||
                "         CRJE," ||
                "         QCJE," ||
                "         ZRSZ," ||
                "         ZCSZ," ||
                "         ZFZ," ||
                "         ZFZ_SR," ||
                "         CCCB," ||
                "         DRYK," ||
                "         CAST((CASE" ||
                "                WHEN ZZC_SR = 0 AND ZZC > 0 THEN" ||
                "                 ZZC - ZQSZ_FLT - DRYK" ||
                "                ELSE" ||
                "                 TZZH_FE" ||
                "              END) AS DECIMAL(16, 2)) AS TZZH_FE," ||
                "         CAST((CASE" ||
                "                WHEN ZZC_SR = 0 AND ZZC > 0 THEN" ||
                "                 ZZC - ZQSZ_FLT - DRYK" ||
                "                ELSE" ||
                "                 TZZH_FE_SR" ||
                "              END) AS DECIMAL(16, 2)) AS TZZH_FE_SR," ||
                "         CAST((CASE" ||
                "                WHEN ZZC_SR = 0 AND ZZC > 0 THEN" ||
                "                 ZZC - ZQSZ_FLT - DRYK" ||
                "                ELSE" ||
                "                 TZZH_ZXFE" ||
                "              END) AS DECIMAL(16, 2)) AS TZZH_ZXFE," ||
                "         CAST((CASE" ||
                "                WHEN ZZC_SR = 0 AND ZZC > 0 THEN" ||
                "                 ZZC - ZQSZ_FLT - DRYK" ||
                "                ELSE" ||
                "                 TZZH_ZXFE_SR" ||
                "              END) AS DECIMAL(16, 2)) AS TZZH_ZXFE_SR," ||
                "         CAST((CASE" ||
                "                WHEN ZZC_SR = 0 AND ZZC > 0 THEN" ||
                "                 1.0" ||
                "                ELSE" ||
                "                 1.0" ||
                "              END) AS DECIMAL(16, 10)) AS TZZH_ZXJZ," ||
                "         CAST((CASE" ||
                "                WHEN ZZC_SR = 0 AND ZZC > 0 THEN" ||
                "                 1.0" ||
                "                ELSE" ||
                "                 1.0" ||
                "              END) AS DECIMAL(16, 10)) AS TZZH_ZXJZ_SR," ||
                "         CAST((CASE" ||
                "                WHEN ZZC_SR = 0 AND ZZC > 0 THEN" ||
                "                1.0" ||
                "                ELSE" ||
                "                 1.0" ||
                "              END) AS DECIMAL(16, 8)) AS TZZH_LJJZ," ||
                "         (CASE" ||
                "           WHEN ZZC_SR = 0 AND ZZC > 0 THEN" ||
                "            1.0" ||
                "           ELSE" ||
                "            1.0" ||
                "         END) AS TZZH_LJJZ_SR," ||
                "         ZQSZ_JRCP," ||
                "         LXSR," ||
                "         ZCZJ_TOTC," ||
                "         ZRZJ_FOTC," ||
                "         LXZC_GPZY," ||
                "         JYFY_GPZY," ||
                "         LXZC_QT," ||
                "         QTSR, " ||
                "         QTZC_FY, " ||
                "         JYL," ||
                I_RQ || " as rq" ||
                "    FROM (SELECT NVL(TZZH_ID, 0) AS TZZH_ID," ||
                "                 t.KHH," ||
                "                 CAST(NVL(ZZC_SR, 0) AS DECIMAL(16, 2)) AS ZZC_SR," ||
                "                 CAST(NVL(ZZC, 0) AS DECIMAL(16, 2)) AS ZZC," ||
                "                 CAST(NVL(ZQSZ, 0) AS DECIMAL(16, 2)) AS ZQSZ," ||
                "                 CAST(NVL(ZQSZ_FLT, 0) AS DECIMAL(16, 2)) AS ZQSZ_FLT," ||
                "                 CAST(NVL(ZQSZ_DYP, 0) AS DECIMAL(16, 2)) AS ZQSZ_DYP," ||
                "                 CAST(NVL(ZJYE, 0) AS DECIMAL(16, 2)) AS ZJYE," ||
                "                 CAST(NVL(ZTZC, 0) AS DECIMAL(16, 2)) AS ZTZC," ||
                "                 CAST(NVL(QTZC, 0) AS DECIMAL(16, 2)) AS QTZC," ||
                "                 CAST(NVL(CRJE, 0) AS DECIMAL(16, 2)) AS CRJE," ||
                "                 CAST(NVL(QCJE, 0) AS DECIMAL(16, 2)) AS QCJE," ||
                "                 CAST(NVL(ZRSZ, 0) AS DECIMAL(16, 2)) AS ZRSZ," ||
                "                 CAST(NVL(ZCSZ, 0) AS DECIMAL(16, 2)) AS ZCSZ," ||
                "                 CAST(NVL(ZFZ_SR, 0) AS DECIMAL(16, 2)) AS ZFZ_SR," ||
                "                 CAST(NVL(ZFZ, 0) AS DECIMAL(16, 2)) AS ZFZ," ||
                "                 CAST(NVL(CCCB, 0) AS DECIMAL(16, 2)) AS CCCB," ||
                "                 0 AS DRYK," ||
                "                 CAST(ROUND(NVL(TZZH_FE, 0), 2) AS DECIMAL(16, 2)) AS TZZH_FE," ||
                "                 CAST(NVL(TZZH_FE_SR, 0) AS DECIMAL(16, 2)) AS TZZH_FE_SR," ||
                "                 CAST(NVL(TZZH_ZXFE, 0) AS DECIMAL(16, 2)) AS TZZH_ZXFE," ||
                "                 CAST(NVL(TZZH_ZXFE_SR, 0) AS DECIMAL(16, 2)) AS TZZH_ZXFE_SR," ||
                "                 CAST(IF(NVL(TZZH_ZXFE, 0) = 0," ||
                "                         1.0," ||
                "                         ROUND(NVL(ZZC - ZQSZ_FLT, 0) / TZZH_ZXFE, 10)) AS" ||
                "                      DECIMAL(16, 10)) AS TZZH_ZXJZ," ||
                "                 CAST(ROUND(NVL(TZZH_ZXJZ_SR, 0),10) AS DECIMAL(16, 10)) AS TZZH_ZXJZ_SR," ||
                "                 CAST(IF(NVL(TZZH_FE, 0) = 0," ||
                "                         1.0," ||
                "                         ROUND(NVL(ZZC - ZQSZ_FLT, 0) / TZZH_FE, 8)) AS" ||
                "                      DECIMAL(16, 8)) AS TZZH_LJJZ," ||
                "                 CAST(NVL(TZZH_LJJZ_SR, 0) AS DECIMAL(16, 8)) AS TZZH_LJJZ_SR," ||
                "                 CAST(NVL(ZQSZ_JRCP, 0) AS DECIMAL(16, 2)) AS ZQSZ_JRCP," ||
                "                 CAST(NVL(LXSR, 0) AS DECIMAL(16, 2)) AS LXSR," ||
                "                 CAST(NVL(ZCZJ_TOTC, 0) AS DECIMAL(16, 2)) AS ZCZJ_TOTC," ||
                "                 CAST(NVL(ZRZJ_FOTC, 0) AS DECIMAL(16, 2)) AS ZRZJ_FOTC," ||
                "                 CAST(NVL(LXZC_GPZY, 0) AS DECIMAL(16, 2)) AS LXZC_GPZY," ||
                "                 CAST(NVL(JYFY_GPZY, 0) AS DECIMAL(16, 2)) AS JYFY_GPZY," ||
                "                 CAST(NVL(LXZC_QT, 0) AS DECIMAL(16, 2)) AS LXZC_QT," ||
                "                 CAST(NVL(QTSR, 0) AS DECIMAL(16, 2)) AS QTSR," ||
                "                 CAST(NVL(QTZC_FY, 0) AS DECIMAL(16, 2)) AS QTZC_FY," ||
                "                 CAST(NVL(JYL, 0) AS DECIMAL(16, 2)) AS JYL" ||
                "            FROM (SELECT TZZH_ID," ||
                "                        s.KHH," ||
                "                        NVL(SUM(ZZC_SR), 0) AS ZZC_SR," ||
                "                        NVL(SUM(ZQSZ), 0) + NVL(SUM(ZJYE), 0) +" ||
                "                        NVL(SUM(ZQSZ_FLT), 0) + NVL(SUM(ZQSZ_DYP), 0) +" ||
                "                        NVL(SUM(ZTZC), 0) + NVL(SUM(QTZC), 0) -" ||
                "                        NVL(SUM(ZFZ), 0) AS ZZC," ||
                "                        NVL(SUM(ZQSZ), 0) AS ZQSZ," ||
                "                        NVL(SUM(ZQSZ_FLT), 0) AS ZQSZ_FLT," ||
                "                        NVL(SUM(ZQSZ_DYP), 0) AS ZQSZ_DYP," ||
                "                        NVL(SUM(ZJYE), 0) AS ZJYE," ||
                "                        NVL(SUM(ZTZC), 0) AS ZTZC," ||
                "                        NVL(SUM(QTZC), 0) AS QTZC," ||
                "                        NVL(SUM(CRJE), 0) AS CRJE," ||
                "                        NVL(SUM(QCJE), 0) AS QCJE," ||
                "                        NVL(SUM(ZRSZ), 0) AS ZRSZ," ||
                "                        NVL(SUM(ZCSZ), 0) AS ZCSZ," ||
                "                        NVL(SUM(ZFZ_SR), 0) AS ZFZ_SR," ||
                "                        NVL(SUM(ZFZ), 0) AS ZFZ," ||
                "                        NVL(SUM(CCCB), 0) AS CCCB," ||
                "                        CASE" ||
                "                          WHEN NVL(SUM(NVL(TZZH_LJJZ_SR,0)), 1.0) = 0 THEN" ||
                "                           NVL(SUM(ZQSZ), 0) + NVL(SUM(ZJYE), 0) +" ||
                "                           NVL(SUM(ZQSZ_DYP), 0) +" ||
                "                           NVL(SUM(ZTZC), 0) + NVL(SUM(QTZC), 0) -" ||
                "                           NVL(SUM(ZFZ), 0)" ||
                "                          ELSE" ||
                "                           IF(NVL(SUM(NVL(TZZH_ZXFE_SR,0)),0)=0," ||
                "                              NVL(SUM(ZQSZ), 0) + NVL(SUM(ZJYE), 0) +" ||
                "                           NVL(SUM(ZQSZ_FLT), 0) + NVL(SUM(ZQSZ_DYP), 0) +" ||
                "                              NVL(SUM(ZTZC), 0) + NVL(SUM(QTZC), 0) -" ||
                "                              NVL(SUM(ZFZ), 0)," ||
                "                              NVL(SUM(TZZH_FE_SR), 0) +" ||
                "                              (NVL(SUM(CRJE), 0) + NVL(SUM(LXSR), 0) +" ||
                "                               NVL(SUM(ZRZJ_FOTC), 0) -" ||
                "                               NVL(SUM(QCJE), 0) -" ||
                "                               NVL(SUM(ZCZJ_TOTC), 0) -" ||
                "                               NVL(SUM(LXZC_GPZY), 0) -" ||
                "                               NVL(SUM(JYFY_GPZY), 0) -" ||
                "                               NVL(SUM(LXZC_QT), 0) + NVL(SUM(ZRSZ), 0) -" ||
                "                               NVL(SUM(ZCSZ), 0)) /" ||
                "                              NVL(SUM(TZZH_LJJZ_SR), 1.0))" ||
                "                        END AS TZZH_FE," ||
                "                        NVL(SUM(TZZH_FE_SR), 0) AS TZZH_FE_SR," ||
                "                        NVL(SUM(TZZH_LJJZ_SR), 0) AS TZZH_LJJZ_SR," ||
                "                        CASE" ||
                "                          WHEN NVL(SUM(NVL(TZZH_ZXJZ_SR,0)), 1.0) = 0 THEN" ||
                "                           NVL(SUM(ZQSZ), 0) + NVL(SUM(ZJYE), 0) +" ||
                "                           NVL(SUM(ZQSZ_DYP), 0) +" ||
                "                           NVL(SUM(ZTZC), 0) + NVL(SUM(QTZC), 0) -" ||
                "                           NVL(SUM(ZFZ), 0)" ||
                "                          ELSE" ||
                "                           IF(NVL(SUM(NVL(TZZH_ZXFE_SR,0)),0)=0," ||
                "                              NVL(SUM(ZQSZ), 0) + NVL(SUM(ZJYE), 0) +" ||
                "                              NVL(SUM(ZTZC), 0) + NVL(SUM(QTZC), 0) -" ||
                "                              NVL(SUM(ZFZ), 0)," ||
                "                              NVL(SUM(TZZH_ZXFE_SR), 0) +" ||
                "                              (NVL(SUM(CRJE), 0) + NVL(SUM(LXSR), 0) +" ||
                "                               NVL(SUM(ZRZJ_FOTC), 0) -" ||
                "                               NVL(SUM(QCJE), 0) -" ||
                "                               NVL(SUM(ZCZJ_TOTC), 0) -" ||
                "                               NVL(SUM(LXZC_GPZY), 0) -" ||
                "                               NVL(SUM(JYFY_GPZY), 0) -" ||
                "                               NVL(SUM(LXZC_QT), 0) + NVL(SUM(ZRSZ), 0) -" ||
                "                               NVL(SUM(ZCSZ), 0)) /" ||
                "                              NVL(SUM(TZZH_ZXJZ_SR), 1.0))" ||
                "                        END AS TZZH_ZXFE," ||
                "                        NVL(SUM(TZZH_ZXFE_SR), 0) AS TZZH_ZXFE_SR," ||
                "                        NVL(SUM(NVL(TZZH_ZXJZ_SR,0)), 0) AS TZZH_ZXJZ_SR," ||
                "                        NVL(SUM(ZQSZ_JRCP), 0) AS ZQSZ_JRCP," ||
                "                        NVL(SUM(LXSR), 0) AS LXSR," ||
                "                        NVL(SUM(ZCZJ_TOTC), 0) AS ZCZJ_TOTC," ||
                "                        NVL(SUM(ZRZJ_FOTC), 0) AS ZRZJ_FOTC," ||
                "                        NVL(SUM(LXZC_GPZY), 0) AS LXZC_GPZY," ||
                "                        NVL(SUM(JYFY_GPZY), 0) AS JYFY_GPZY," ||
                "                        NVL(SUM(LXZC_QT), 0) AS LXZC_QT," ||
                "                        NVL(SUM(QTSR), 0) AS QTSR," ||
                "                        NVL(SUM(QTZC_FY), 0) AS QTZC_FY," ||
                "                        NVL(SUM(JYL), 0) AS JYL" ||
                "                   FROM  " || F_IDS_GET_TABLENAME('sparkStatDR', NULL) || " s " ||
                "                  GROUP BY s.TZZH_ID, s.KHH) t left join cust.t_khxx_jjyw k on (t.khh=k.khh)" ||
                "                  WHERE k.khrq <=  " || I_RQ || " 
                              and (k.khzt != '3' or (k.khzt = '3' and k.xhrq >= " || I_RQ || " ))" ||
                ") T";
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
 
  -- 净值折算
  BEGIN
 l_tableName := F_IDS_GET_TABLENAME('sparkStatDRResult_zsjz', NULL); 
 l_sqlBuf :="select  DR.TZZH_ID, 
                     DR.KHH, 
                     DR.ZZC_SR, 
                     DR.ZZC, 
                     DR.ZQSZ, 
                     DR.zqsz_flt,     
                     DR.zqsz_dyp,      
                     DR.ZJYE, 
                     DR.ZTZC, 
                     DR.QTZC, 
                     DR.CRJE, 
                     DR.QCJE, 
                     DR.ZRSZ, 
                     DR.ZCSZ, 
                     DR.ZFZ, 
                     DR.ZFZ_SR, 
                     DR.CCCB, 
                     DR.DRYK, 
                     DR.TZZH_FE,
                     DR.TZZH_FE_SR,
                     -- 使用折算后份额
                     zs.DQFE AS TZZH_ZXFE, 
                     DR.TZZH_ZXFE_SR, 
                     -- 使用折算后净值
                     zs.DQJZ AS TZZH_ZXJZ, 
                     DR.TZZH_ZXJZ_SR, 
                     -- 使用折算后滚动净值替代累计净值
                     zs.GDJZ AS TZZH_LJJZ, 
                     DR.TZZH_LJJZ_SR, 
                     DR.ZQSZ_JRCP, 
                     DR.LXSR, 
                     DR.ZCZJ_TOTC, 
                     DR.ZRZJ_FOTC, 
                     DR.LXZC_GPZY, 
                     DR.JYFY_GPZY, 
                     DR.LXZC_QT, 
                     DR.QTSR,    
                     DR.QTZC_FY, 
                     DR.JYL, 
                     -- 新增字段存储折算标志、cache、折算前份额和净值，便于数据核对
                     zs.jzzzl,
                     zs.reload,
                     zs.cache,
                     DR.TZZH_ZXFE AS TZZH_ZXFE_OLD,
                     DR.TZZH_ZXJZ AS TZZH_ZXJZ_OLD,
                     DR.TZZH_LJJZ AS TZZH_LJJZ_OLD,
                     DR.rq
    from "|| F_IDS_GET_TABLENAME('sparkStatDRResult', NULL) ||" dr
    left join (SELECT   ConvertWorth(z.khh, 
         nvl(z.zzc,0.0), 
         nvl(z.zfz,0.0), 
         nvl(z.crje,0.0), 
         nvl(z.qcje,0.0), 
         nvl(z.zrsz,0.0), 
         nvl(z.zcsz,0.0), 
         nvl(z.dryk,0.0), 
         nvl(z.qtsr+z.lxsr,0.0), 
         nvl(NULL,0.0), 
         nvl(NULL,0.0), 
         nvl(NULL, 1.0), 
         nvl(NULL,''))  -- 初始化直接赋值null
         FROM  "||F_IDS_GET_TABLENAME('sparkStatDRResult', NULL) ||" z) zs 
        on dr.khh = zs.khh ";
 F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
 
 END;


    F_IDS_OVERWRITE_PARTITION(l_tableName, 'cust', 't_stat_zc_r', I_RQ, NULL);

  END;
END;
/