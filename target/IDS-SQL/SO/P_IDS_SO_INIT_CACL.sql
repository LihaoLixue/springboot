!set plsqlUseSlash true
CREATE OR REPLACE procedure cust.p_ids_so_init_cacl(
  --输入变量
  I_RQ IN INT

) is

/******************************************************************
  *文件名称：CUST.P_IDS_SO_INIT_CACL
  *项目名称：IDS计算
  *文件说明：个股期权-清算初始化

  创建人：苏雁南
  功能说明：个股期权-清算初始化

  参数说明

  修改者    版本号        修改日期        说明
  苏雁南     v1.0.0        2019/6/15           创建
  王睿驹     v1.0.1    2019/8/20           根据java代码修改
  燕居庆     v1.0.2     2019/9/16           对标java-ids 4858版本
                                            1.根据持仓方向调整证券市值
                                            2.调整最新份额/最新净值计算的净资产为总资产
*******************************************************************/
l_sqlBuf STRING; --创建表语句
l_tableName STRING; --临时表名
l_sqlWhereCurrentDay STRING; 
l_sqlWhereLastDay STRING;
l_lastDay STRING;
l_hlcsHKD DECIMAL(12,6);
l_hlcsUSD DECIMAL(12,6);
BEGIN
  
  l_sqlWhereCurrentDay := I_RQ;
  
  BEGIN
  /**
   * 临时表名
   * 由于集中/两融/期权可能存在相同临时表，因此在改造时，需要添加相关前缀：
   * 集中：F_IDS_GET_TABLENAME('sparkGpzyDhghy', NULL);
   * 两融：F_IDS_GET_TABLENAME('xy_sparkGpzyDhghy', NULL);
   * 期权：F_IDS_GET_TABLENAME('so_sparkGpzyDhghy', NULL);
   */
  l_tableName := F_IDS_GET_TABLENAME('so_sparkHyccGT', NULL);
  l_sqlBuf := 'select * from dsc_bas.t_so_zqye_his where rq = ' || l_sqlWhereCurrentDay;
  
  /**
   * 调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
   */
  F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName)
  END;
  
  BEGIN
  /**
   * 临时表名
   * 由于集中/两融/期权可能存在相同临时表，因此在改造时，需要添加相关前缀：
   * 集中：F_IDS_GET_TABLENAME('sparkGpzyDhghy', NULL);
   * 两融：F_IDS_GET_TABLENAME('xy_sparkGpzyDhghy', NULL);
   * 期权：F_IDS_GET_TABLENAME('so_sparkHyhq', NULL);
   */
  l_tableName := F_IDS_GET_TABLENAME('so_sparkHyhq', NULL);
  l_sqlBuf := 'select * from cust.t_so_hyhq_his where rq = ' || l_sqlWhereCurrentDay;
  
  /**
   * 调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
   */
  F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
  END;
  
  BEGIN
  l_tableName := F_IDS_GET_TABLENAME('so_sparkHyccResult', NULL);
  l_sqlBuf := "select " ||
        "   z.khh," ||
        "   z.gdh," ||
        "   z.zzhbm," ||
        "   z.jys," ||
        "   z.hydm," ||
        "   z.hymc," ||
        "   z.bz," ||
        I_RQ||" as kcrq," ||
        "   z.qqlx," ||
        "   z.ccfx," ||
        "   z.BDBQ," ||
        "   z.zqsl," ||
        "   case when z.ccfx='2' then 0-case when NVL(z.zqsl*q.jydw*q.zxj,0)=0 then z.zxsz else z.zqsl*q.jydw*q.zxj end" ||
        "        else case when NVL(z.zqsl*q.jydw*q.zxj,0)=0 then z.zxsz else z.zqsl*q.jydw*q.zxj end" ||
        "   end as zxsz," || 
        "   z.kcsl," ||
        "   case when nvl(z.kcsl*q.jydw*q.zxj,0)=0 then cast(round(z.zxsz/z.zqsl*z.kcsl,2) as decimal(16,2)) else cast(round(z.kcsl*q.jydw*q.zxj,2) as decimal(16,2)) end as kcje," ||
        "   z.pcsl," ||
        "   case when nvl(z.pcsl*q.jydw*q.zxj,0)=0 then cast(round(z.zxsz/z.zqsl*z.pcsl,2) as decimal(16,2)) else cast(round(z.pcsl*q.jydw*q.zxj,2) as decimal(16,2)) end as pcje,"||
        "   case when nvl(z.zqsl*q.jydw*q.zxj,0)=0 then z.zxsz else z.zqsl*q.jydw*q.zxj end as cccb," ||
        "   case when nvl(z.zqsl*q.jydw*q.zxj,0)=0 then z.zxsz else z.zqsl*q.jydw*q.zxj end/z.zqsl as cbj," ||
        "   0 as ljyk," ||
        "   case when nvl(z.zqsl*q.jydw*q.zxj,0)=0 then z.zxsz else z.zqsl*q.jydw*q.zxj end as tbcccb," ||
        "   case when nvl(z.zqsl*q.jydw*q.zxj,0)=0 then z.zxsz else z.zqsl*q.jydw*q.zxj end/z.zqsl as tbcbj," ||
        "   0 as dryk," ||
        "   z.rq" ||
        " from "|| F_IDS_GET_TABLENAME("so_sparkHyccGT", NULL) ||" z left join "|| F_IDS_GET_TABLENAME("so_sparkHyhq", NULL) ||" q on (z.jys=q.jys and z.hydm=q.hydm) where z.zqsl!=0";
          
  F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
  /**
   * 写入分区表
   * 入参：临时表名，目标库名，目标表名，目标表分区字段值，客户号
   * F_IDS_OVERWRITE_PARTITION(tablename, dbname, partitionCloumn, partitionValue, khh)
   */ 
  F_IDS_OVERWRITE_PARTITION(l_tableName, "CUST", "t_so_zqye_his", I_RQ, NULL);

  END;
  
  BEGIN
  l_tableName := F_IDS_GET_TABLENAME('so_sparkZqyeCbjsGT', NULL);
  l_sqlBuf := "select " ||
        "   khh," ||
        "   gdh," ||
        "   zzhbm," ||
        "   jys," ||
        "   hydm," ||
        "   bz," ||
        "   kcrq," ||
        "   zqsl," ||
        "   zxsz as zqsz," ||
        "   ccfx as mmfx," ||
        "   BDBQ," ||
        "   kcsl as drkcsl," ||
        "   kcje as drkcje," ||
        "   pcsl as drpcsl," ||
        "   pcje as drpcje," ||
        "   kcsl as ljkcsl," ||
        "   kcje as ljkcje," ||
        "   pcsl as ljpcsl," ||
        "   pcje as ljpcje," ||
        "   cccb," ||
        "   dryk," ||
        "   ljyk," ||
        "   rq" ||
        " from cust.t_so_zqye_his where rq=" || I_RQ;
          
   F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
  /**
   * 写入分区表
   * 入参：临时表名，目标库名，目标表名，目标表分区字段值，客户号
   * F_IDS_OVERWRITE_PARTITION(tablename, dbname, partitionCloumn, partitionValue, khh)
   */ 
  F_IDS_OVERWRITE_PARTITION(l_tableName, "CUST", "t_so_zqye_cbjs", I_RQ, NULL);
 END;
 BEGIN
  /**
   * 资金余额
   */
  l_tableName := F_IDS_GET_TABLENAME('so_sparkZjye', NULL);
  l_sqlBuf := 'select * from cust.t_so_zjye_his where rq = ' || l_sqlWhereCurrentDay;
  
  F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName)
 END;
 BEGIN
  /**
   * 证券余额
   */
  l_tableName := F_IDS_GET_TABLENAME('so_sparkZqyeZctj', NULL);
  l_sqlBuf := 'select khh,gdh,zzhbm,jys,hydm,hymc,bz,kcrq,qqlx,ccfx,bdbq,zqsl,abs(zxsz) as zxsz,kcsl,kcje,pcsl,pcje,cccb,cbj,ljyk,tbcccb,tbcbj,dryk,rq from cust.t_so_zqye_his where rq = ' || l_sqlWhereCurrentDay;
  
  F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName)
 END;
 BEGIN
  -- 获取汇率参数
  SELECT CAST(F_GET_HLCS('2',I_RQ) AS DECIMAL(12,6)) INTO l_hlcsHKD FROM `SYSTEM`.dual;
  SELECT CAST(F_GET_HLCS('3',I_RQ) AS DECIMAL(12,6)) INTO l_hlcsUSD FROM `SYSTEM`.dual;
 END;
 BEGIN
  
  l_tableName := F_IDS_GET_TABLENAME('so_sparkZctjDR', NULL);

  --生成证券市值数据
  l_sqlBuf := "SELECT    " ||
                "        KHH,  " ||
                "        0 AS zzc_sr       ," ||
                "        0 AS zzc         ," ||
                "        0 AS jzc_sr," ||
                "        0 AS jzc," ||
                "        CAST(ROUND(SUM(" ||
                "           CASE WHEN CCFX='2' THEN -ZXSZ ELSE ZXSZ END * " ||
                "           CASE WHEN BZ = '2' THEN " || l_hlcsHKD || 
                "                WHEN BZ = '3' THEN  " || l_hlcsUSD || 
                "                ELSE 1 END) " ||
                "        ,2) AS DECIMAL(16,2)) AS ZQSZ,  " ||
                "        0 AS zqsz_kt_sr," ||
                "        CAST(ROUND(SUM(CASE  WHEN CCFX='2' THEN " ||
                "                CASE WHEN BZ = '2' THEN  " ||
                "                    ABS(ZXSZ) * " || l_hlcsHKD ||
                "                WHEN BZ = '3' THEN  " ||
                "                    ABS(ZXSZ) * " || l_hlcsUSD ||
                "                ELSE  " ||
                "                    ABS(ZXSZ)  " ||
                "            END ELSE 0 END),2) AS DECIMAL(16,2)) AS zqsz_kt,  " ||
                "        0 AS cccb_kt_sr," ||
                "        CAST(ROUND(SUM(CASE  WHEN CCFX='2' THEN " ||
                "                CASE WHEN BZ = '2' THEN  " ||
                "                    CCCB * " || l_hlcsHKD ||
                "                WHEN BZ = '3' THEN  " ||
                "                    CCCB * " || l_hlcsUSD ||
                "                ELSE  " ||
                "                    CCCB  " ||
                "            END ELSE 0 END),2) AS DECIMAL(16,2)) AS cccb_kt,  " ||
                "        0 AS zjye         ," ||
                "        0 AS ztzc         ," ||
                "        0 AS qtzc         ," ||
                "        0 AS crje         ," ||
                "        0 AS qcje         ," ||
                "        0 AS zrsz         ," ||
                "        0 AS zcsz         ," ||
                "        0 AS zfz         ," ||
                "        0 AS zfz_sr       ," ||
                "        CAST(ROUND(SUM(CASE  " ||
                "                WHEN BZ = '2' THEN  " ||
                "                    NVL(CCCB,0) * " || l_hlcsHKD ||
                "                WHEN BZ = '3' THEN  " ||
                "                    NVL(CCCB,0) * " || l_hlcsUSD ||
                "                ELSE  " ||
                "                    NVL(CCCB,0)  " ||
                "            END),2) AS DECIMAL(16,2)) AS CCCB,  " ||
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
                "        0 AS jyl         " ||
        "FROM "|| F_IDS_GET_TABLENAME("so_sparkZqyeZctj", NULL) ||"  T" ||
        " GROUP BY KHH";
  --生成资金余额
   l_sqlBuf :=l_sqlBuf||" UNION ALL "||
         "SELECT " ||
        "    KHH,  " ||
        "    0 AS zzc_sr     ," ||
        "    0 AS zzc     ," ||
        "    0 AS jzc_sr," ||
        "    0 AS jzc," ||
        "    0 AS zqsz     ," ||
        "    0 AS zqsz_kt_sr," ||
        "    0 AS zqsz_kt," ||
        "    0 AS cccb_kt_sr," ||
        "    0 AS cccb_kt," ||
        "    CAST(ROUND(SUM(CASE  " ||
        "        WHEN BZ = '2' THEN  " ||
        "          ZHYE * " || l_hlcsHKD ||
        "        WHEN BZ = '3' THEN  " ||
        "          ZHYE * " || l_hlcsUSD ||
        "        ELSE  " ||
        "          ZHYE  " ||
        "      END),2) AS DECIMAL(16,2)) AS ZJYE,  " ||
        "    0 AS ztzc     ," ||
        "    0 AS qtzc     ," ||
        "    0 AS crje     ," ||
        "    0 AS qcje     ," ||
        "    0 AS zrsz     ," ||
        "    0 AS zcsz     ," ||
        "    0 AS zfz     ," ||
        "    0 AS zfz_sr     ," ||
        "    0 AS cccb     ," ||
        "    0 AS dryk     ," ||
        "    0 AS tzzh_fe   ," ||
        "    0 AS tzzh_fe_sr   ," ||
        "    0 AS tzzh_zxfe   ," ||
        "    0 AS tzzh_zxfe_sr ," ||
        "    0 AS tzzh_zxjz   ," ||
        "    0 AS tzzh_zxjz_sr ," ||
        "    0 AS tzzh_ljjz   ," ||
        "    0 AS tzzh_ljjz_sr ," ||
        "    0 AS lxsr     ," ||
        "    0 AS lxzc   ," ||
        "    0 AS jyl     " ||
        " FROM "|| F_IDS_GET_TABLENAME("so_sparkZjye", NULL) || " T  " ||
        " GROUP BY KHH";
  F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName)
 END;
 BEGIN
  l_tableName := F_IDS_GET_TABLENAME('so_sparkZctjResult', NULL);
  l_sqlBuf := "SELECT " ||
        "  KHH,  " ||
        "  CAST(ROUND((CASE WHEN ZZC_SR = 0 AND ZZC > 0 THEN ZZC - DRYK ELSE ZZC_SR END),2) AS DECIMAL(16,2)) AS ZZC_SR,  " ||
        "  CAST(ROUND(ZZC,2) AS DECIMAL(16,2)) AS ZZC,  " ||
        "  CAST(ROUND(JZC_SR,2) AS DECIMAL(16,2)) AS JZC_SR,  " ||
        "  CAST(ROUND(JZC,2) AS DECIMAL(16,2)) AS JZC,  " ||
        "  CAST(ROUND(ZQSZ,2) AS DECIMAL(16,2)) AS ZQSZ,  " ||
        "  CAST(ROUND(ZQSZ_KT_SR,2) AS DECIMAL(16,2)) AS ZQSZ_KT_SR,  " ||
        "  CAST(ROUND(ZQSZ_KT,2) AS DECIMAL(16,2)) AS ZQSZ_KT,  " ||
        "  CAST(ROUND(CCCB_KT_SR,2) AS DECIMAL(16,2)) AS CCCB_KT_SR,  " ||
        "  CAST(ROUND(CCCB_KT,2) AS DECIMAL(16,2)) AS CCCB_KT,  " ||
        "  CAST(ROUND(ZJYE,2) AS DECIMAL(16,2)) AS ZJYE,  " ||
        "  CAST(ROUND(ZTZC,2) AS DECIMAL(16,2)) AS ZTZC,  " ||
        "  CAST(ROUND(QTZC,2) AS DECIMAL(16,2)) AS QTZC,  " ||
        "  CAST(ROUND(CRJE,2) AS DECIMAL(16,2)) AS CRJE,  " ||
        "  CAST(ROUND(QCJE,2) AS DECIMAL(16,2)) AS QCJE,  " ||
        "  CAST(ROUND(ZRSZ,2) AS DECIMAL(16,2)) AS ZRSZ,  " ||
        "  CAST(ROUND(ZCSZ,2) AS DECIMAL(16,2)) AS ZCSZ,  " ||
        "  CAST(ROUND(ZFZ,2) AS DECIMAL(16,2)) AS ZFZ,  " ||
        "  CAST(ROUND(ZFZ_SR,2) AS DECIMAL(16,2)) AS ZFZ_SR,  " ||
        "  CAST(ROUND(CCCB,2) AS DECIMAL(16,2)) AS CCCB,  " ||
        "  CAST(ROUND(DRYK,2) AS DECIMAL(16,2)) AS DRYK,  " ||
        "  CAST(ROUND((CASE WHEN ZZC_SR = 0 AND ZZC > 0 THEN ZZC - ZFZ - DRYK ELSE TZZH_FE END),2) AS DECIMAL(16,2)) AS TZZH_FE,  " ||
        "  CAST(ROUND((CASE WHEN ZZC_SR = 0 AND ZZC > 0 THEN ZZC - ZFZ - DRYK ELSE TZZH_FE_SR END),2) AS DECIMAL(16,2)) AS TZZH_FE_SR,  " ||
        "  CAST(ROUND((CASE WHEN ZZC_SR = 0 AND ZZC > 0 THEN ZZC - ZFZ - DRYK ELSE TZZH_ZXFE END),2) AS DECIMAL(16,2)) AS TZZH_ZXFE,  " ||
        "  CAST(ROUND((CASE WHEN ZZC_SR = 0 AND ZZC > 0 THEN ZZC - ZFZ - DRYK ELSE TZZH_ZXFE_SR END),2) AS DECIMAL(16,2)) AS TZZH_ZXFE_SR,  " ||
        "  CAST(ROUND((CASE WHEN ZZC_SR = 0 AND ZZC > 0 THEN 1.0 ELSE 1.0 END),4) AS DECIMAL(10,4)) AS TZZH_ZXJZ,  " ||
        "  CAST(ROUND((CASE WHEN ZZC_SR = 0 AND ZZC > 0 THEN 1.0 ELSE 1.0 END),4) AS DECIMAL(10,4)) AS TZZH_ZXJZ_SR,  " ||
        "  CAST(ROUND((CASE WHEN ZZC_SR = 0 AND ZZC > 0 THEN 1.0 ELSE 1.0 END),4) AS DECIMAL(22,4)) AS TZZH_LJJZ,  " ||
        "  CAST(ROUND((CASE WHEN ZZC_SR = 0 AND ZZC > 0 THEN 1.0 ELSE 1.0 END),4) AS DECIMAL(22,4)) AS TZZH_LJJZ_SR,  " ||
        "  CAST(ROUND(LXSR,2) AS DECIMAL(16,2)) AS LXSR,  " ||
        "  CAST(ROUND(LXZC,2) AS DECIMAL(16,2)) AS LXZC,  " ||
        "  CAST(ROUND(JYL,2) AS DECIMAL(16,2)) AS JYL,  " ||
        I_RQ || " as RQ" ||
        " FROM  " ||
        " (SELECT " ||
        "  T.KHH,  " ||
        "  nvl(ZZC_SR, 0) AS ZZC_SR,  " ||
        "  nvl(ZZC, 0) AS ZZC,  " ||
        "  nvl(JZC_SR, 0) AS JZC_SR,  " ||
        "  nvl(JZC, 0) AS JZC,  " ||
        "  nvl(ZQSZ, 0) AS ZQSZ,  " ||
        "  nvl(ZQSZ_KT_SR, 0) AS ZQSZ_KT_SR,  " ||
        "  nvl(ZQSZ_KT, 0) AS ZQSZ_KT,  " ||
        "  nvl(CCCB_KT_SR, 0) AS CCCB_KT_SR,  " ||
        "  nvl(CCCB_KT, 0) AS CCCB_KT,  " ||
        "  nvl(ZJYE, 0) AS ZJYE,  " ||
        "  nvl(ZTZC, 0) AS ZTZC,  " ||
        "  nvl(QTZC, 0) AS QTZC,  " ||
        "  nvl(CRJE, 0) AS CRJE,  " ||
        "  nvl(QCJE, 0) AS QCJE,  " ||
        "  nvl(ZRSZ, 0) AS ZRSZ,  " ||
        "  nvl(ZCSZ, 0) AS ZCSZ,  " ||
        "  nvl(ZFZ_SR, 0) AS ZFZ_SR,  " ||
        "  nvl(ZFZ, 0) AS ZFZ,  " ||
        "  nvl(CCCB, 0) AS CCCB,  " ||
        "  0 AS DRYK,  " ||
        "  ROUND(nvl(TZZH_FE,0),2) AS TZZH_FE,  " ||
        "  nvl(TZZH_FE_SR, 0) AS TZZH_FE_SR,  " ||
        "  nvl(TZZH_ZXFE,0) AS TZZH_ZXFE,  " ||
        "  nvl(TZZH_ZXFE_SR, 0) AS TZZH_ZXFE_SR,  " ||
        "  IF(nvl(TZZH_ZXFE,0) = 0, 1.0, ROUND((nvl(ZZC,0)-nvl(ZFZ,0)) / TZZH_ZXFE,4)) AS TZZH_ZXJZ,  " ||
        "  nvl(TZZH_ZXJZ_SR, 0) AS TZZH_ZXJZ_SR,  " ||
        "  IF(nvl(TZZH_FE,0) = 0, 1.0, ROUND((nvl(ZZC,0)-nvl(ZFZ,0)) / TZZH_FE,4)) AS TZZH_LJJZ,  " ||
        "  nvl(TZZH_LJJZ_SR, 0) AS TZZH_LJJZ_SR,  " ||
        "  nvl(LXSR, 0) AS LXSR,  " ||
        "  nvl(LXZC, 0) AS LXZC,  " ||
        "  nvl(JYL, 0) AS JYL " ||
        " FROM  " ||
        "  (SELECT " ||
        "    T.KHH,  " ||
        "    nvl(SUM(ZZC_SR), 0) AS ZZC_SR,  " ||
        "    nvl(SUM(ZQSZ), 0) + nvl(SUM(ZJYE), 0) + nvl(SUM(ZTZC), 0) + nvl(SUM(QTZC), 0) AS ZZC,  " ||
        "    nvl(SUM(JZC_SR), 0) AS JZC_SR,  " ||
        "    nvl(SUM(ZQSZ), 0) + nvl(SUM(ZJYE), 0) + nvl(SUM(ZTZC), 0) + nvl(SUM(QTZC), 0) AS JZC,  " ||
        "    nvl(SUM(ZQSZ), 0) AS ZQSZ,  " ||
        "    nvl(SUM(ZQSZ_KT_SR), 0) AS ZQSZ_KT_SR,  " ||
        "    nvl(SUM(ZQSZ_KT), 0) AS ZQSZ_KT,  " ||
        "    nvl(SUM(CCCB_KT_SR), 0) AS CCCB_KT_SR,  " ||
        "    nvl(SUM(CCCB_KT), 0) AS CCCB_KT,  " ||
        "    nvl(SUM(ZJYE), 0) AS ZJYE,  " ||
        "    nvl(SUM(ZTZC), 0) AS ZTZC,  " ||
        "    nvl(SUM(QTZC), 0) AS QTZC,  " ||
        "    nvl(SUM(CRJE), 0) AS CRJE,  " ||
        "    nvl(SUM(QCJE), 0) AS QCJE,  " ||
        "    nvl(SUM(ZRSZ), 0) AS ZRSZ,  " ||
        "    nvl(SUM(ZCSZ), 0) AS ZCSZ,  " ||
        "    nvl(SUM(ZFZ_SR), 0) AS ZFZ_SR,  " ||
        "    nvl(SUM(ZFZ), 0) AS ZFZ,  " ||
        "    nvl(SUM(CCCB), 0) AS CCCB,  " ||
        "    CASE   " ||
        "        WHEN nvl(SUM(TZZH_LJJZ_SR), 1.0)=0 THEN  " ||
        --如果上日累计净值为0，取本日资产作为份额
        "      nvl(SUM(ZQSZ), 0) + nvl(SUM(ZJYE), 0) +  " ||
        "        nvl(SUM(ZTZC), 0) + nvl(SUM(QTZC), 0) - nvl(SUM(ZFZ),0)  " ||
        "    ELSE   " ||
        "      IF(SUM(TZZH_ZXFE_SR) is NULL,  " ||
        "          nvl(SUM(ZQSZ), 0) + nvl(SUM(ZJYE), 0) +  " ||
        "          nvl(SUM(ZTZC), 0) + nvl(SUM(QTZC), 0) - nvl(SUM(ZFZ),0) ,  " ||
        "          nvl(SUM(TZZH_ZXFE_SR),0) ||  " ||
        "          (nvl(SUM(CRJE),0) + nvl(SUM(LXSR),0) - nvl(SUM(QCJE),0) + nvl(SUM(ZRSZ),0) - nvl(SUM(ZCSZ),0)) / nvl(SUM(TZZH_ZXJZ_SR), 1.0))  " ||
        "    END AS TZZH_FE,  " ||
        "    nvl(SUM(TZZH_FE_SR), 0) AS TZZH_FE_SR,  " ||
        "    nvl(SUM(TZZH_LJJZ_SR), 0) AS TZZH_LJJZ_SR,  " ||
        "    CASE   " ||
        "        WHEN nvl(SUM(TZZH_ZXJZ_SR), 1.0)=0 THEN  " ||
        --如果上日累计净值为0，取本日资产作为最新份额
        "      nvl(SUM(ZQSZ), 0) + nvl(SUM(ZJYE), 0) ||  " ||
        "        nvl(SUM(ZTZC), 0) + nvl(SUM(QTZC), 0)- nvl(SUM(ZFZ),0)  " ||
        "    ELSE  " ||
        "      IF(SUM(TZZH_ZXFE_SR) is NULL,  " ||
        "          nvl(SUM(ZQSZ), 0) + nvl(SUM(ZJYE), 0) +  " ||
        "          nvl(SUM(ZTZC), 0) + nvl(SUM(QTZC), 0) - nvl(SUM(ZFZ),0) ,  " ||
        "          nvl(SUM(TZZH_ZXFE_SR),0) ||  " ||
        "          (nvl(SUM(CRJE),0) + nvl(SUM(LXSR),0) - nvl(SUM(QCJE),0) + nvl(SUM(ZRSZ),0) - nvl(SUM(ZCSZ),0)) / nvl(SUM(TZZH_ZXJZ_SR), 1.0))  " ||
        "    END AS TZZH_ZXFE,  " ||
        "    nvl(SUM(TZZH_ZXFE_SR), 0) AS TZZH_ZXFE_SR,  " ||
        "    nvl(SUM(TZZH_ZXJZ_SR), 0) AS TZZH_ZXJZ_SR,  " ||
        "    nvl(SUM(LXSR), 0) AS LXSR,  " ||
        "    nvl(SUM(LXZC), 0) AS LXZC,  " ||
        "    nvl(SUM(JYL), 0) AS JYL" ||
        "  FROM "|| F_IDS_GET_TABLENAME("so_sparkZctjDR", NULL) ||" T " ||
        "  GROUP BY  KHH) T LEFT JOIN CUST.T_KHXX_JJYW A " ||
        " ON A.KHH = T.KHH WHERE NOT(A.XHRQ IS NOT NULL AND A.XHRQ< " || I_RQ || "))A";
          
   F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
  /**
   * 写入分区表
   * 入参：临时表名，目标库名，目标表名，目标表分区字段值，客户号
   * F_IDS_OVERWRITE_PARTITION(tablename, dbname,targetTable, partitionValue, khh)
   */ 
  F_IDS_OVERWRITE_PARTITION(l_tableName, "CUST", "t_stat_so_zc_r", I_RQ, NULL);
 END;
end;
/