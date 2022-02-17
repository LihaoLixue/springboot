!set plsqlUseSlash true
CREATE OR REPLACE procedure cust.p_ids_so_cost_calcution(
  --输入变量
  I_RQ IN INT,
  I_KHH IN STRING

) is

/******************************************************************
  *文件名称：CUST.P_IDS_SO_COST_CALCUTION
  *项目名称：IDS计算
  *文件说明：个股期权-成本盈亏修正处理

  创建人：陈亚楠
  功能说明：个股期权-成本盈亏修正处理

  参数说明

  修改者        版本号            修改日期            说明
  陈亚楠       v1.0.0           2019/6/25            创建
  王睿驹       v1.0.1            2019/8/20            根据java代码修改
  燕居庆       v1.0.2            2019/9/16           对标java-ids 4858版本
                                                       1.调整市值计算
*******************************************************************/
l_sqlBuf STRING; --创建表语句
l_tableName STRING; --临时表名
l_sqlWhereCurrentDay STRING; 
l_sqlWhereLastDay STRING;
l_lastDay STRING;
BEGIN
  
  -- 获取上一交易日
  SELECT F_GET_JYR_DATE(I_RQ, -1) INTO l_lastDay FROM system.dual;
  
  IF I_KHH IS NULL THEN
    l_sqlWhereCurrentDay := I_RQ;
    l_sqlWhereLastDay := l_lastDay;
  ELSE 
    l_sqlWhereCurrentDay := I_RQ || ' and khh = ' || I_KHH;
    l_sqlWhereLastDay := l_lastDay || ' and khh = ' || I_KHH;
  END IF;
  
  -----------------------------        //0、初始化源数据----------------------------------- 
  BEGIN
    l_tableName := F_IDS_GET_TABLENAME('so_sparkJgmxls', I_KHH);
    l_sqlBuf := 'select * from cust.t_so_jgmxls_his where cjrq = ' || l_sqlWhereCurrentDay;
    
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
    l_tableName := F_IDS_GET_TABLENAME('so_sparkHyhq' , I_KHH);
    l_sqlBuf := 'select * from cust.t_so_hyhq_his where rq = ' || I_RQ;
    
    /**
     * 调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
     */
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
  END; 
  
  BEGIN

    l_tableName := F_IDS_GET_TABLENAME('so_sparkHydm', I_KHH);
    l_sqlBuf := 'select * from cust.t_so_hydm';
    
    /**
     * 调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
     */
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
  END;
  
  BEGIN
    /**
     * 临时表名
     * 由于集中/两融/期权可能存在相同临时表，因此在改造时，需要添加相关前缀：
     * 集中：F_IDS_GET_TABLENAME('sparkHycc', I_KHH);
     * 两融：F_IDS_GET_TABLENAME('xy_sparkHycc', I_KHH);
     * 期权：F_IDS_GET_TABLENAME('so_sparkHycc', I_KHH);
     */
    l_tableName := F_IDS_GET_TABLENAME('so_sparkHycc', I_KHH);
    l_sqlBuf := 'select * from dsc_bas.t_so_zqye_his z where rq = ' || l_sqlWhereCurrentDay;--  T日
    
    /**
     * 调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
     */
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName)
  END;
  
  BEGIN

    l_tableName := F_IDS_GET_TABLENAME('so_sparkHYCbjsPre1', I_KHH);
    l_sqlBuf := 'select * from CUST.T_SO_ZQYE_CBJS D where rq = ' || l_sqlWhereLastDay;--  T-1日
    
    /**
     * 调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
     */
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName)
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
    l_sqlBuf := 'select khh,gdh,zzhbm,jys,hydm,hymc,bz,kcrq,qqlx,ccfx,bdbq,zqsl,abs(zxsz) as zxsz,kcsl,kcje,pcsl,pcje,cccb,cbj,ljyk,tbcccb,tbcbj,dryk,rq from CUST.T_SO_ZQYE_HIS D where rq = ' || l_sqlWhereLastDay;--  T-1日
    
    /**
     * 调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
     */
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName)
  END;
  
   /*1、开平仓盈亏分析
        KPBZ              KCSL   KCJE      PCSL   PCJE
        开仓(O)             YSSL   abs(YSJE)
        平仓(C)                             YSSL   abs(YSJE)
        摘牌(G)                             YSSL   0
        行权(E)                             YSSL   0
        现金结算行权                         YSSL   abs(YSJE)
  */
  -----------------------------        //1、开平仓盈亏分析----------------------------------- 
  BEGIN
  l_tableName := F_IDS_GET_TABLENAME('so_sparkHyCbjs_1', I_KHH);
    l_sqlBuf:="SELECT
                T.KHH,
                T.GDH,
                T.ZZHBM,
                T.JYS,
                T.HYDM,
                T.BZ,
                CASE KPBZ WHEN 'C' THEN CASE MMFX WHEN '1' THEN '2' ELSE '1' END ELSE MMFX END MMFX,
        T.BDBQ,
                SUM(CASE WHEN KPBZ='O' THEN ABS(YSJE)
                WHEN KPBZ IN ('G','E')
                THEN CASE WHEN T.YSSL>0 THEN ABS(T.YSSL)*CASE WHEN HQ.ZXJ>0 THEN HQ.ZXJ ELSE HQ.ZSP END ELSE 0 END
                ELSE 0 END) DRKCJE,
                SUM(CASE WHEN KPBZ='O' THEN ABS(YSSL)
                WHEN KPBZ IN ('G','E')
                THEN CASE WHEN T.YSSL>0 THEN T.YSSL ELSE 0 END
                ELSE 0 END) DRKCSL,
                SUM(CASE WHEN KPBZ='C' THEN ABS(YSJE)
                WHEN ((KPBZ='E' AND CJBH='期权行权后注销') OR (KPBZ='G' AND CJBH IN ('行权注销','仓位对冲','净持仓处理','到期注销')))
                               THEN CASE WHEN T.YSSL<0 THEN ABS(T.YSSL)*CASE WHEN HQ.ZXJ>0 THEN HQ.ZXJ ELSE HQ.ZSP END ELSE 0 END
                             ELSE 0 END) DRPCJE,
                    SUM(CASE WHEN KPBZ='C' THEN ABS(YSSL)
                             WHEN KPBZ IN ('G','E')
                               THEN CASE WHEN T.YSSL<0 THEN ABS(T.YSSL) ELSE 0 END
                             ELSE 0 END) DRPCSL,
                    SUM(S1+S2+S3+S4+S5+S6) AS DRJYFY
                 FROM  "|| F_IDS_GET_TABLENAME('so_sparkJgmxls',I_KHH) ||"  T 
                 left join "|| F_IDS_GET_TABLENAME('so_sparkHyhq', I_KHH) ||" hq on (t.JYS=hq.JYS and t.HYDM=hq.HYDM)
                 GROUP BY T.KHH, T.GDH,T.ZZHBM, T.JYS, T.HYDM, T.BZ, T.BDBQ,
                 CASE KPBZ WHEN 'C' THEN CASE MMFX WHEN '1' THEN '2' ELSE '1' END ELSE MMFX END";
       F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);

  END;
 
  -----------------------------        //2、将本日数据和上日累计数据进行累计处理----------------------------------- 
  BEGIN
      l_tableName := F_IDS_GET_TABLENAME('so_sparkHyCbjs_2', I_KHH);
        l_sqlBuf := "SELECT KHH,
                        GDH,
                        ZZHBM,
                        JYS,
                        HYDM,
                        BZ,
                        MMFX,
            BDBQ,
                       nvl(SUM(DRKCSL), 0) DRKCSL,
                       nvl(SUM(DRKCJE), 0) DRKCJE,
                       nvl(SUM(DRPCSL), 0) DRPCSL,
                       nvl(SUM(DRPCJE), 0) DRPCJE,
                       nvl(SUM(DRJYFY), 0) DRJYFY,
                       nvl(SUM(LJKCSL), 0) LJKCSL,
                       nvl(SUM(LJKCJE), 0) LJKCJE,
                       nvl(SUM(LJPCSL), 0) LJPCSL,
                       nvl(SUM(LJPCJE), 0) LJPCJE,
                       nvl(SUM(LJJYFY), 0) LJJYFY
                  FROM (SELECT KHH,
                               GDH,
                               ZZHBM,
                               JYS,
                               HYDM,
                               BZ,
                               MMFX,
                 BDBQ,
                               SUM(DRKCSL) AS DRKCSL,
                               SUM(DRKCJE) AS DRKCJE,
                               SUM(DRPCSL) AS DRPCSL,
                               SUM(DRPCJE) AS DRPCJE,
                               SUM(DRJYFY) AS DRJYFY,
                               SUM(DRKCSL) AS LJKCSL,
                               SUM(DRKCJE) AS LJKCJE,
                               SUM(DRPCSL) AS LJPCSL,
                               SUM(DRPCJE) AS LJPCJE,
                               SUM(DRJYFY) AS LJJYFY
                          FROM "|| F_IDS_GET_TABLENAME('so_sparkHyCbjs_1', I_KHH) ||"
                         GROUP BY KHH, GDH, ZZHBM, JYS, HYDM, BZ, MMFX, BDBQ
                        UNION ALL
                        SELECT KHH,
                               GDH,
                               ZZHBM,
                               JYS,
                               HYDM,
                               BZ,
                               MMFX,
                 BDBQ,
                               0 AS DRKCSL,
                               0 AS DRKCJE,
                               0 AS DRPCSL,
                               0 AS DRPCJE,
                               0 AS DRJYFY,
                               IF(nvl(ZQSL, 0) = 0, 0, nvl(LJKCSL, 0)) LJKCSL,
                               IF(nvl(ZQSL, 0) = 0, 0, nvl(LJKCJE, 0)) LJKCJE,
                               IF(nvl(ZQSL, 0) = 0, 0, nvl(LJPCSL, 0)) LJPCSL,
                               IF(nvl(ZQSL, 0) = 0, 0, nvl(LJPCJE, 0)) LJPCJE,
                               IF(nvl(ZQSL, 0) = 0, 0, nvl(LJJYFY, 0)) LJJYFY 
                          FROM "|| F_IDS_GET_TABLENAME('so_sparkHYCbjsPre1', I_KHH) ||" where zqsl!=0) D
                 GROUP BY KHH, GDH, ZZHBM, JYS, HYDM, BZ, MMFX, BDBQ";
        F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
    END;
    
  -----------------------------        //3、获取当日的份额市值以及上日的成本盈亏等数据----------------------------------- 
  BEGIN
      l_tableName := F_IDS_GET_TABLENAME('so_sparkHyCbjs_3', I_KHH);
    l_sqlBuf := "SELECT "|| I_RQ ||" AS RQ," ||
                    "       A.KHH," ||
                    "       A.GDH," ||
                    "       A.ZZHBM," ||
                    "       A.JYS," ||
                    "       A.HYDM," ||
                    "       A.BZ," ||
                    "       A.MMFX," ||
                    "       A.BDBQ," ||
                    "       nvl(B.ZQSL, 0) AS ZQSL," ||
                    "       nvl(B.ZXSZ, 0) AS ZQSZ," ||
                    "       nvl(A.DRKCSL, 0) AS DRKCSL," ||
                    "       nvl(A.DRKCJE, 0) AS DRKCJE," ||
                    "       nvl(A.DRPCSL, 0) AS DRPCSL," ||
                    "       nvl(A.DRPCJE, 0) AS DRPCJE," ||
                    "       nvl(A.DRJYFY, 0) AS DRJYFY," ||
                    "       CASE" ||
                    "           WHEN nvl(C.ZQSL, 0) = 0 AND nvl(DRKCSL, 0) = nvl(DRPCSL, 0) THEN" ||
                    "            nvl(DRKCJE, 0)" ||
                    "           ELSE" ||
                    "            nvl(LJKCJE, 0)" ||
                    "       END AS LJKCJE," || -- 考虑当日买入卖出清仓情况处理
                    "       CASE" ||
                    "           WHEN nvl(C.ZQSL, 0) = 0 AND nvl(DRKCSL, 0) = nvl(DRPCSL, 0) THEN" ||
                    "            nvl(DRKCSL, 0)" ||
                    "           ELSE" ||
                    "            nvl(LJKCSL, 0)" ||
                    "       END AS LJKCSL," || -- 考虑当日买入卖出清仓情况处理
                    "       CASE" ||
                    "           WHEN nvl(C.ZQSL, 0) = 0 AND nvl(DRKCSL, 0) = nvl(DRPCSL, 0) THEN" ||
                    "            nvl(DRPCJE, 0)" ||
                    "           ELSE" ||
                    "            nvl(LJPCJE, 0)" ||
                    "       END AS LJPCJE," || -- 考虑当日买入卖出清仓情况处理
                    "       CASE" ||
                    "           WHEN nvl(C.ZQSL, 0) = 0 AND nvl(DRKCSL, 0) = nvl(DRPCSL, 0) THEN" ||
                    "            nvl(DRPCSL, 0)" ||
                    "           ELSE" ||
                    "            nvl(LJPCSL, 0)" ||
                    "       END AS LJPCSL," || -- 考虑当日买入卖出清仓情况处理
                    "       CASE" ||
                    "           WHEN nvl(C.ZQSL, 0) = 0 AND nvl(DRKCSL, 0) = nvl(DRPCSL, 0) THEN" ||
                    "            nvl(DRJYFY, 0)" ||
                    "           ELSE" ||
                    "            nvl(LJJYFY, 0)" ||
                    "       END AS LJJYFY," || -- 考虑当日买入卖出清仓情况处理
                    "       nvl(C.CCCB, 0) SRCCCB," ||
                    "       nvl(C.LJYK, 0) SRLJYK," ||
                    "       nvl(C.ZQSL, 0) SRZQSL," ||
                    "       nvl(C.ZXSZ, 0) SRZQSZ," ||
                    "       ROUND(IF(nvl(C.ZQSL, 0) > 0, nvl(C.CCCB, 0) / nvl(C.ZQSL, 0), 0), 4) AS SRCBJ" ||
                    "  FROM "|| F_IDS_GET_TABLENAME('so_sparkHyCbjs_2', I_KHH) ||" A" ||
                    "  LEFT JOIN "|| F_IDS_GET_TABLENAME('so_sparkHycc', I_KHH) ||" B" ||
                    "    ON (A.KHH = B.KHH" ||
                    "   AND A.GDH = B.GDH" ||
                    "   AND A.ZZHBM = B.ZZHBM" ||
                    "   AND A.JYS = B.JYS" ||
                    "   AND A.HYDM = B.HYDM" ||
                    "   AND A.BDBQ = B.BDBQ" ||
                    "   AND A.MMFX = B.CCFX)" ||
                    "  LEFT JOIN "|| F_IDS_GET_TABLENAME('so_sparkHyccPre1',I_KHH) ||" C" ||
                    "    ON A.KHH = C.KHH" ||
                    "   AND A.GDH = C.GDH"  ||
                    "   AND A.ZZHBM = C.ZZHBM"  ||
                    "   AND A.JYS = C.JYS"  ||
                    "   AND A.HYDM = C.HYDM" ||
                    "   AND A.BDBQ = C.BDBQ" ||
                    "   AND A.MMFX = C.CCFX";
         F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
   END;
   
   -----------------------------        //4、生成最终数据----------------------------------- 
   BEGIN
  l_tableName := F_IDS_GET_TABLENAME('so_sparkHyCbjsResult', I_KHH);
        l_sqlBuf := "SELECT " ||
                "           T1.KHH," ||
                "           T1.GDH," ||
                "           T1.ZZHBM," ||
                "           T1.JYS," ||
                "           T1.HYDM," ||
                "           T1.BZ," ||
                "           CASE" ||
                "    WHEN (T1.KCRQ IS NULL OR T1.KCRQ = 0) AND T2.KCRQ IS NOT NULL THEN" ||
                "     T2.KCRQ" ||
                "    ELSE" ||
                "     T1.KCRQ" ||
                "           END AS KCRQ," ||
                "           T1.ZQSL," ||
                "           T1.BDBQ," ||
                "           CAST(T1.ZQSZ AS DECIMAL(16,2)) AS ZQSZ," ||
                "           T1.MMFX," ||
                "           T1.DRKCSL," ||
                "           CAST(T1.DRKCJE AS DECIMAL(16,2)) AS DRKCJE," ||
                "           T1.DRPCSL," ||
                "           CAST(T1.DRPCJE AS DECIMAL(16,2)) AS DRPCJE," ||
                "           CAST(T1.DRJYFY AS DECIMAL(16,2)) AS DRJYFY," ||
                "           T1.LJKCSL," ||
                "           CAST(T1.LJKCJE AS DECIMAL(16,2)) AS LJKCJE," ||
                "           T1.LJPCSL," ||
                "           CAST(T1.LJPCJE AS DECIMAL(16,2)) AS LJPCJE," ||
                "           CAST(T1.LJJYFY AS DECIMAL(16,2)) AS LJJYFY," ||
                "           T1.SRZQSL," ||
                "           CAST(T1.SRZQSZ AS DECIMAL(16,2)) AS SRZQSZ," ||
                "           CAST(T1.SRCBJ AS DECIMAL(9,4)) AS SRCBJ," ||
                "           CAST(T1.SRCCCB AS DECIMAL(16,2)) AS SRCCCB," ||
                "           CAST(T1.SRLJYK AS DECIMAL(16,2)) AS SRLJYK," ||
                "           CAST(T1.CCCB AS DECIMAL(16,2)) AS CCCB," ||
                "           CAST(T1.DRYK AS DECIMAL(16,2)) AS DRYK," ||
                "           CAST(T1.LJYK AS DECIMAL(16,2)) AS LJYK," ||
                "           T1.RQ" ||
                "      FROM (SELECT " ||
                "        T.RQ," ||
                "        T.KHH," ||
                "        T.GDH," ||
                "        T.ZZHBM," ||
                "        T.JYS," ||
                "        T.HYDM," ||
                "        T.BZ," ||
                "        T.ZQSL," ||
                "        T.BDBQ," ||
                "        T.ZQSZ," ||
                "        T.MMFX," ||
                "        T.DRKCJE," ||
                "        T.DRKCSL," ||
                "        T.DRPCJE," ||
                "        T.DRPCSL," ||
                "        T.DRJYFY," ||
                "        T.LJKCJE," ||
                "        T.LJKCSL," ||
                "        T.LJPCJE," ||
                "        T.LJPCSL," ||
                "        T.LJJYFY," ||
                "        T.SRCCCB," ||
                "        T.SRLJYK," ||
                "        T.SRZQSL," ||
                "        T.SRZQSZ," ||
                "        T.SRCBJ," ||
                "        ROUND(CASE" ||
                "                  WHEN DRKCSL > DRPCSL THEN" ||
                "                   SRCCCB + DRKCJE - DRPCJE" ||
                "                  WHEN DRKCSL < DRPCSL THEN" ||
                "                   SRCCCB - SRCBJ * (DRPCSL - DRKCSL)" ||
                "                  ELSE" ||
                "                   SRCCCB" ||
                "              END," ||
                "              2) AS CCCB," ||
                "        ROUND(CASE" ||
                "                  WHEN DRKCSL > DRPCSL THEN" ||
                "                   SRLJYK" ||
                "                  WHEN DRKCSL < DRPCSL AND MMFX='1' THEN" ||
                "                   SRLJYK + (DRPCJE - DRKCJE) - SRCBJ * (DRPCSL - DRKCSL)" ||
                "                  WHEN DRKCSL < DRPCSL AND MMFX='2' THEN" ||
                "                   SRLJYK + SRCBJ * (DRPCSL-DRKCSL) - (DRPCJE-DRKCJE)" ||
                "                  WHEN DRKCSL = DRPCSL AND MMFX='1' THEN" ||
                "                   SRLJYK + (DRPCJE - DRKCJE)" ||
                "                  WHEN DRKCSL = DRPCSL AND MMFX='2' THEN" ||
                "                   SRLJYK + (DRKCJE - DRPCJE)" ||
                "                  ELSE" ||
                "                   nvl(SRLJYK,0)" ||
                "              END," ||
                "              2) AS LJYK," ||
                "        CASE WHEN MMFX='1' THEN T.ZQSZ - T.SRZQSZ -(DRKCJE - DRPCJE) WHEN MMFX='2' THEN (DRKCJE-DRPCJE) - (T.ZQSZ - T.SRZQSZ) ELSE 0 END AS DRYK," ||
                "        CASE" ||
                "            WHEN T.SRZQSL=0 THEN" ||
                "             T.RQ" ||
                "            ELSE" ||
                "             0" ||
                "        END AS KCRQ" ||
                "              FROM "|| F_IDS_GET_TABLENAME('so_sparkHyCbjs_3', I_KHH) ||" T) T1" ||
                "      LEFT JOIN (SELECT KHH, GDH, ZZHBM, JYS, HYDM, BZ, MMFX,BDBQ, KCRQ" ||
                "        FROM "|| F_IDS_GET_TABLENAME('so_sparkHYCbjsPre1', I_KHH)  ||" A" ||
                "       WHERE A.ZQSL > 0" ||
                "         AND A.KCRQ <> 0) T2" ||
                "        ON T1.KHH = T2.KHH" ||
                "       AND T1.GDH = T2.GDH" ||
                "       AND T1.ZZHBM = T2.ZZHBM" ||
                "       AND T1.JYS = T2.JYS" ||
                "       AND T1.HYDM = T2.HYDM" ||
                "       AND T1.BDBQ = T2.BDBQ" ||
                "       AND T1.MMFX = T2.MMFX";
            F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
        END;
        
    -----------------------      //5、更新到表-----------------------------------------------
  BEGIN
    F_IDS_OVERWRITE_PARTITION(l_tableName, "CUST", "t_so_zqye_cbjs", I_RQ, I_KHH);
  END; 
  
  BEGIN
    l_tableName := F_IDS_GET_TABLENAME('so_sparkHyccResult', I_KHH);
    l_sqlBuf := "SELECT T.KHH," ||
                  "       T.GDH," ||
                  "       T.ZZHBM," ||
                  "       T.JYS," ||
                  "       T.HYDM," ||
                  "       T.HYMC," ||
                  "       T.BZ," ||
                  "       D.KCRQ AS KCRQ," ||
                  "       T.QQLX," ||
                  "       T.CCFX," ||
                  "       T.BDBQ," ||
                  "       T.ZQSL," ||
                  "       CASE WHEN T.CCFX='2' THEN -T.ZXSZ ELSE T.ZXSZ END AS ZXSZ," ||
                  "       T.KCSL," ||
                  "       nvl(D.LJKCJE,0) AS KCJE," ||
                  "       T.PCSL," ||
                  "       nvl(D.LJPCJE,0) AS PCJE," ||
                  "       nvl(D.CCCB, 0) AS CCCB," ||
                  "       nvl(D.CCCB, 0) / T.ZQSL AS CBJ," ||
                  "       nvl(D.LJYK, 0) AS LJYK," ||
                  "       nvl(D.TBCCCB, 0) AS TBCCCB," ||
                  "       CASE" ||
                  "           WHEN T.ZQSL = 0 THEN" ||
                  "            0" ||
                  "           ELSE" ||
                  "            nvl(D.TBCCCB, 0) / T.ZQSL" ||
                  "       END AS TBCBJ," ||
                  "       nvl(D.DRYK, 0) AS DRYK," ||
                  "       rq" ||
                  "  FROM "|| F_IDS_GET_TABLENAME('so_sparkHycc', I_KHH) ||" T" ||
                  "  LEFT JOIN (SELECT KHH," ||
                  "         GDH," ||
                  "         ZZHBM," ||
                  "         JYS," ||
                  "         HYDM," ||
                  "         MMFX," ||
                  "         BDBQ," ||
                  "         KCRQ," ||
                  "         LJKCJE," ||
                  "         LJPCJE," ||
                  "         CCCB," ||
                  "         LJYK," ||
                  "         DRYK," ||
                  "         CCCB - LJYK AS TBCCCB" ||
                  "    FROM "|| F_IDS_GET_TABLENAME('so_sparkHyCbjsResult', I_KHH) ||" ) D" ||
                  "    ON T.KHH = D.KHH" ||
                  "   AND T.GDH = D.GDH" ||
                  "   AND T.ZZHBM = D.ZZHBM" ||
                  "   AND T.JYS = D.JYS" ||
                  "   AND T.HYDM = D.HYDM" ||
                  "   AND T.CCFX = D.MMFX" ||
                  "   AND T.BDBQ = D.BDBQ" ||
                  "   AND T.ZQSL != 0";
         F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
  END;
  
  BEGIN
    F_IDS_OVERWRITE_PARTITION(l_tableName, "CUST", "t_so_zqye_his", I_RQ, I_KHH);
  END;
  
  BEGIN
    l_tableName := F_IDS_GET_TABLENAME('so_sparkHyTzsyResult', I_KHH);
    l_sqlBuf := " SELECT " ||
                "           D.KCRQ," ||
                "           D.KHH," ||
                "           D.GDH," ||
                "           D.ZZHBM," ||
                "           D.JYS," ||
                "           D.HYDM," ||
                "           T.QQLX," ||
                "           D.MMFX AS CCFX," ||
                "           D.BDBQ," ||
                "           D.BZ," ||
                "           D.LJKCJE AS KCJE," ||
                "           D.LJPCJE AS PCJE," ||
                "           D.LJJYFY AS JYFY," ||
                "           D.LJYK AS LJYK," ||
                "           D.DRYK," ||
                I_RQ || " as qcrq" ||
                "     FROM "||  F_IDS_GET_TABLENAME('so_sparkHyCbjsResult', I_KHH) ||" D" ||
                "      LEFT JOIN "||  F_IDS_GET_TABLENAME('so_sparkHydm', I_KHH) ||" T" ||
                "        ON T.JYS = D.JYS" ||
                "       AND T.HYDM = D.HYDM" ||
                "     WHERE D.ZQSL = 0" ||
                "       AND D.DRPCSL>0";
         F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
  END;
  
  BEGIN
    F_IDS_OVERWRITE_PARTITION(l_tableName, "CUST", "T_SO_TZSY", I_RQ, I_KHH);
  END;

end;
/
