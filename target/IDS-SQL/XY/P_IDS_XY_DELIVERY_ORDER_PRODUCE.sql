!set plsqlUseSlash true
create or replace procedure cust.p_ids_xy_delivery_order_produce(
  --输入变量
  I_RQ IN INT,
  I_KHH IN STRING

) is

/******************************************************************
  *文件名称：CUST.P_IDS_XY_DELIVERY_ORDER_PRODUCE
  *项目名称：IDS计算
  *文件说明：融资融券-交割单清算后处理

  创建人：胡阳明
  功能说明：融资融券-交割单清算后处理

  参数说明

  修改者        版本号        修改日期        说明
  胡阳明        v1.0.0        2019/6/24       创建
*******************************************************************/
l_sqlBuf STRING; --创建表语句
l_tableName STRING; --临时表名
l_sqlWhereCurrentDay STRING; 
l_sqlWhereLastDay STRING;
l_lastDay INT;
l_sql STRING;
l_khh STRING;
l_hlcsHKD DECIMAL(12,6);
l_hlcsUSD DECIMAL(12,6);
TYPE nest_table IS TABLE OF STRING;
l_tableArr nest_table DEFAULT NULL;
l_columns STRING;
BEGIN
  -- 获取上一交易日
  SELECT F_GET_JYR_DATE(I_RQ, -1) INTO l_lastDay FROM system.dual;
  
  -- 临时表创建temp库下 
  IF I_KHH IS NULL THEN
    l_sqlWhereLastDay := l_lastDay;
    l_sqlWhereCurrentDay := I_RQ;
  ELSE 
    l_sqlWhereLastDay := l_lastDay || ' and khh = ' || I_KHH;
    l_sqlWhereCurrentDay := I_RQ || ' and khh = ' || I_KHH;
  END IF;
  
  BEGIN
  -----------------------------------------   //1、获取上日持仓--------------------------------------
    /**
     * 临时表名
     * 由于集中/两融/期权可能存在相同临时表，因此在改造时，需要添加相关前缀：
     * 集中：F_IDS_GET_TABLENAME('sparkzqyeXY', I_KHH);
     * 两融：F_IDS_GET_TABLENAME('xy_sparkzqyeXY', I_KHH);
     * 期权：F_IDS_GET_TABLENAME('so_sparkzqyeXY', I_KHH);
     */
    l_tableName := F_IDS_GET_TABLENAME('xy_sparkzqyeXY', I_KHH);
    l_sqlBuf := 'select * from CUST.t_xy_zqye_his D where rq = ' || l_sqlWhereLastDay;
    
    /**
     * 调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
     */
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName)
  END;
  
 ------------------------------------ //2、获取到两融的交割单----------------------------------------- 
  BEGIN
    /**
     * 临时表名
     * 由于集中/两融/期权可能存在相同临时表，因此在改造时，需要添加相关前缀：
     * 集中：F_IDS_GET_TABLENAME('sparkJgmxlsXY', I_KHH);
     * 两融：F_IDS_GET_TABLENAME('xy_sparkJgmxlsXY', I_KHH);
     * 期权：F_IDS_GET_TABLENAME('so_sparkJgmxlsXY', I_KHH);
     */
    l_tableName := F_IDS_GET_TABLENAME('xy_sparkJgmxlsXY', I_KHH);
    l_sqlBuf := 'select * from cust.t_xy_jgmxls_his where cjrq = ' || l_sqlWhereCurrentDay;
    
    /**
     * 调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
     */
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
  END;
  
------------------------------------//3、统一处理交割单，暂时不区分分级基金,生成Dataset供后续迭代使用-------------------------  
  
  BEGIN
    l_tableName := F_IDS_GET_TABLENAME('xy_jgmxlsXY', I_KHH);
    l_sqlBuf := 
                "SELECT  T.lsh,T.tzzh_id,T.seqno,T.wth,T.khh,T.khxm,T.gdh,T.jys,T.bz,T.yyb,T.zqdm,T.zqmc,T.zqlb,T.jylb," ||
                "    CASE WHEN T.JYLB = '43' AND T.CJRQ>T.JSRQ AND T.JSRQ<>0 THEN 'SHJSCG' WHEN T.JYLB = '43' AND T.JSRQ=0 THEN 'SHFQWT' " ||
                "    WHEN T.JYLB = '30' AND T.CJRQ<T.JSRQ THEN 'SHFQWT' WHEN T.JYLB = '42' AND T.JSRQ=0 THEN 'SGFQWT'  ELSE T.CJBH END AS cjbh," ||
                "    T.cjrq,T.cjsj,T.sbsj,T.cjbs,T.cjsl,T.cjjg,T.jsj,T.lxjg," ||
                "    (CASE WHEN (T.JYLB = '31' OR (T.ZQLB IN ('A0','C0','A3','C3') AND T.JYLB IN ('18','19'))) AND T.CJJE=0 THEN T.CJSL*Z.ZXJ ELSE T.CJJE END) AS cjje,T.lxje," ||
                "    (CASE WHEN T.CJJE=0 AND T.ZQLB='E0' AND T.JYLB='30' AND H.JJDM IS NOT NULL THEN T.CJSL*nvl(H.JJJZ,100)" ||
                "          WHEN T.ZQLB = 'A0' AND T.JYLB = '31' AND T.CJJE=0 THEN T.CJSL*Z.ZXJ  WHEN T.ZQLB IN ('A0','C0','A3','C3') AND T.JYLB IN ('18','19') AND T.CJJE=0 THEN T.CJSL*Z.ZXJ " ||
                "     ELSE T.YSJE END) AS ysje," ||
                "    T.jsrq,T.bczqye,T.YSSL AS yssl," ||
                "    CASE WHEN T.ZQLB IN ('EH') AND T.JYLB='29' THEN 20000000000+T.SEQNO " ||
                "       WHEN (T.JYLB='19' AND T.CJBH='折算变更' AND T.ZQLB = 'L3' AND D.JJLX='1') OR (T.JYLB='18' AND T.CJBH='折算变更' AND T.ZQLB = 'A0' AND D.JJLX<>'1') THEN  " || l_lastDay ||
                "       ELSE if(WTH=0,10000000000+T.SEQNO,wth) END  AS seqno_qs, " || -- 由于深圳货币ETF申购当天有两条交割单，且WTH一模一样，所以特殊处理
                "    T.BCZQYE AS bczqye_qs, T.CJSJ AS cjsj_qs,0 as cccb_qs,0 as cbj_qs,0 as sxyk_qs,0 as ljyk_qs," ||
                "    CASE WHEN (T.JYLB='18' AND T.CJBH='拆分合并' AND D.JJLX='1') OR (T.JYLB='19' AND T.CJBH='拆分合并' AND D.JJLX<>'1' ) THEN '48'" ||
                "      WHEN (T.JYLB='19' AND T.CJBH='拆分合并' AND D.JJLX='1') OR (T.JYLB='18' AND T.CJBH='拆分合并' AND D.JJLX<>'1' ) THEN '47'" ||
                "      WHEN (T.JYLB='19' AND T.CJBH='折算变更' AND T.ZQLB = 'L3' AND D.JJLX='1') THEN '47'" ||
                "      WHEN (T.JYLB='18' AND T.CJBH='折算变更' AND T.ZQLB = 'A0' AND D.JJLX<>'1') THEN '47'" ||
                "      ELSE T.JYLB" ||
                "    END AS jylb_qs," ||
                "    0 AS zt_qs,  " || -- 初始化状态为未清算
                "    0 AS rownum, " || -- 初始化为0
                "    s1,s2,s3,s4,s5,s6," ||
                "    cast(null as string) as remark_qs," ||
                "    cast(0 as decimal(16,2)) as yk_zc," ||
                "    cast(0 as decimal(16,2)) as yk_zr," ||
                "    cast(0 as decimal(16,2)) as jyfy," ||
                "    cast(0 as decimal(16,2)) as cccb_zc," ||
                "    cast(0 as decimal(16,2)) as cccb_zr," ||
                "    ROUND(Z.ZXJ, 4) AS zxj," ||
                "    J.JJJZ AS jjjz" ||
                " FROM " || F_IDS_GET_TABLENAME('xy_sparkJgmxlsXY', I_KHH) || " T" ||
                " LEFT JOIN cust.v_fjjjxx D ON (T.JYS=D.JYS AND T.ZQDM=D.JJDM)" ||
                " LEFT JOIN  DSC_BAS.T_ZQHQ_XZ_HIS Z ON (Z.JYS = T.JYS AND Z.ZQDM = T.ZQDM and z.rq="|| i_rq ||")" ||
                " LEFT JOIN INFO.THIS_JJJZ_HBJJ H ON (H.TADM = T.JYS AND H.JJDM = T.ZQDM and h.rq="||i_rq||")" ||
                " LEFT JOIN DSC_BAS.T_FJJJJZ_HIS J ON (J.TADM = T.JYS AND J.JJDM = T.ZQDM and j.jzrq="||i_rq||")" ||
                " WHERE NOT (T.ZQLB IN ('E0') AND T.JYLB = '29' AND T.SEQNO=0)  " || -- 对ETF申购交割单进行去重处理
                " AND NOT (T.ZQLB IN ('EH') AND T.JYLB = '30' AND T.CJSL=0)";    
                  
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);

  END;
  -----------------------------------统一处理交割单与持仓合并---------------------------------------
  BEGIN
    /**
     * 临时表名
     * 由于集中/两融/期权可能存在相同临时表，因此在改造时，需要添加相关前缀：
     * 集中：F_IDS_GET_TABLENAME('sparkDeliveryHoding', I_KHH);
     * 两融：F_IDS_GET_TABLENAME('xy_sparkDeliveryHoding', I_KHH);
     * 期权：F_IDS_GET_TABLENAME('so_sparkDeliveryHoding', I_KHH);
     */
    l_tableName := F_IDS_GET_TABLENAME('xy_sparkDeliveryHoding', I_KHH);
    l_sqlBuf := " SELECT T.lsh
                      , T.tzzh_id
                      , T.seqno
                      , T.wth
                      , T.khh
                      , T.khxm
                      , T.gdh
                      , T.jys
                      , T.bz
                      , T.yyb
                      , T.zqdm
                      , T.zqmc
                      , T.zqlb
                      , T.jylb
                      , T.cjbh
                      , T.cjrq
                      , T.cjsj
                      , T.sbsj
                      , T.cjbs
                      , T.cjsl
                      , T.cjjg
                      , T.jsj
                      , T.lxjg
                      , T.cjje
                      , T.lxje
                      , T.ysje
                      , T.jsrq
                      , T.bczqye
                      , T.yssl
                      , T.seqno_qs
                      , T.bczqye_qs
                      , T.cjsj_qs
                      , T.cccb_qs
                      , T.cbj_qs
                      , T.sxyk_qs
                      , T.ljyk_qs
                      , T.jylb_qs
                      , T.zt_qs
                      , T.rownum
                      , T.s1
                      , T.s2
                      , T.s3
                      , T.s4
                      , T.s5
                      , T.s6
                      , T.zxj
                      , T.jjjz
                      , T.cccb_zc
                      , T.cccb_zr
                      , T.yk_zc
                      , T.yk_zr
                      , T.jyfy
                      ,T.remark_qs
                      ,  A.ZQSL AS ZQSL_CC,
                     A.CCCB AS CCCB_CC,
                     A.LJYK AS LJYK_CC,
                     A.CBJ  AS CBJ_CC,
                     B.KHH  AS FJ_KHH " ||
            "  FROM " || F_IDS_GET_TABLENAME('xy_jgmxlsXY', I_KHH) || "  T " ||
            "  LEFT JOIN " || F_IDS_GET_TABLENAME('xy_sparkzqyeXY', I_KHH) || " A ON (T.KHH = A.KHH AND T.GDH = A.GDH AND T.JYS = A.JYS AND " ||
            "                      T.ZQDM = A.ZQDM) " ||
            "  LEFT JOIN (SELECT DISTINCT KHH, GDH, JYS, ZQDM " ||
            "               FROM  " || F_IDS_GET_TABLENAME('xy_jgmxlsXY', I_KHH) || "  " ||
            "              WHERE JYLB IN ('47', '48')) B ON (T.KHH = B.KHH AND T.GDH = B.GDH AND " ||
            "                                               T.JYS = B.JYS AND T.ZQDM = B.ZQDM) " ;
    

    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
  END;  
  
   --创建目标表，成本迭代
    BEGIN
	l_tableName := F_IDS_GET_TABLENAME('xy_sparkJgmxlsQsCccb', I_KHH);
	l_sqlBuf := "
		SELECT 
			DeliveryHoding(ROW)
		FROM 
			(SELECT 
				 khh
				  , groupRow(lsh
							  , tzzh_id
							  , seqno
							  , wth
							  , khh
							  , khxm
							  , gdh
							  , jys
							  , bz
							  , yyb
							  , zqdm
							  , zqmc
							  , zqlb
							  , jylb
							  , cjbh
							  , cjrq
							  , cjsj
							  , sbsj
							  , cjbs
							  , cjsl
							  , cjjg
							  , jsj
							  , lxjg
							  , cjje
							  , lxje
							  , ysje
							  , jsrq
							  , bczqye
							  , yssl
							  , seqno_qs
							  , bczqye_qs
							  , cjsj_qs
							  , cccb_qs
							  , cbj_qs
							  , sxyk_qs
							  , ljyk_qs
							  , jylb_qs
							  , zt_qs
							  , rownum
							  , s1
							  , s2
							  , s3
							  , s4
							  , s5
							  , s6
							  , zxj
							  , jjjz
							  , remark_qs
							  , cccb_zc
							  , cccb_zr
							  , yk_zc
							  , yk_zr
							  , jyfy
							  , zqsl_cc
							  , cccb_cc
							  , ljyk_cc
							  , cbj_cc
							  , fj_khh) AS  ROW 
			 FROM " || F_IDS_GET_TABLENAME('xy_sparkDeliveryHoding', I_KHH) ||"
			 GROUP  BY 
				  khh) A";
	
	F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
  END;
  
--------------------------------  //6、生成最后的数据 ---------------------------
  BEGIN
    /**
     * 临时表名
     * 由于集中/两融/期权可能存在相同临时表，因此在改造时，需要添加相关前缀：
     * 集中：F_IDS_GET_TABLENAME('sparkJgmxlsResult', I_KHH);
     * 两融：F_IDS_GET_TABLENAME('xy_sparkJgmxlsResult', I_KHH);
     * 期权：F_IDS_GET_TABLENAME('so_sparkJgmxlsResult', I_KHH);
     */
    l_tableName := F_IDS_GET_TABLENAME('xy_sparkJgmxlsResult', I_KHH);
    l_sqlBuf := 
                "SELECT " ||
                "  t.TZZH_ID     ," ||
                "  t.SEQNO       ," ||
                "  t.WTH         ," ||
                "  t.KHH         ," ||
                "  t.KHXM        ," ||
                "  t.GDH         ," ||
                "  t.JYS         ," ||
                "  t.BZ          ," ||
                "  t.YYB         ," ||
                "  t.ZQDM        ," ||
                "  t.ZQMC        ," ||
                "  t.ZQLB        ," ||
                "  t.JYLB        ," ||
                "  t.CJBH        ," ||
                "  t.CJSJ        ," ||
                "  t.SBSJ        ," ||
                "  t.CJBS        ," ||
                "  t.CJSL        ," ||
                "  t.CJJG        ," ||
                "  t.JSJ         ," ||
                "  t.LXJG        ," ||
                "  t.CJJE        ," ||
                "  t.LXJE        ," ||
                "  t.YSJE        ," ||
                "  t.JSRQ        ," ||
                "  t.BCZQYE      ," ||
                "  t.YSSL        ," ||
                "  t.ZXJ         ," ||
                "  t.JJJZ        ," ||
                "  t.SEQNO_QS    ," ||
                "  t.BCZQYE_QS   ," ||
                "  (CASE WHEN t.JYLB<>'6' AND (t.CJSJ_QS='00\:00\:00' OR t.CJSJ_QS is NULL) THEN '18\:00\:00' ELSE t.CJSJ_QS END) AS CJSJ_QS     ," ||
                "  t.CCCB_QS     ," ||
                "  t.CBJ_QS      ," ||
                "  t.SXYK_QS     ," ||
                "  t.LJYK_QS     ," ||
                "  t.REMARK_QS   ," ||
                "  t.JYLB_QS     ," ||
                "  0 AS ZT_QS       ," ||
                "  CAST(NULL AS DECIMAL(16,2)) AS YK_ZC," ||
                "  CAST(NULL AS DECIMAL(16,2)) AS YK_ZR," ||
                "  t.LSH         ," ||
                "  t.ROWNUM      ," ||
                "  t.S1          ," ||
                "  t.S2          ," ||
                "  t.S3          ," ||
                "  t.S4          ," ||
                "  t.S5          ," ||
                "  t.S6          ," ||
                "  t.JYFY,  " ||
                "  CAST(NULL AS DECIMAL(16,2)) AS CCCB_ZC," ||
                "  CAST(NULL AS DECIMAL(16,2)) AS CCCB_ZR," ||
                I_RQ || " as CJRQ" ||
                "  FROM  " || F_IDS_GET_TABLENAME('xy_sparkJgmxlsQsCccb', I_KHH) || " T LEFT SEMI JOIN (select * from " || F_IDS_GET_TABLENAME('xy_sparkJgmxlsXY', I_KHH) || " where JYLB = '43')D ON " ||
                "  (D.KHH = T.KHH" ||
                " AND D.CJRQ = T.JSRQ" ||
                " AND D.WTH = T.WTH" ||
                " AND D.JYS = T.JYS" ||
                " AND D.ZQDM = T.ZQDM" ||
                " AND D.CJSL = T.CJSL" ||
               -- " AND D.JYLB = '43'" ||
                " AND D.CJRQ < T.CJRQ" ||
                " AND D.KHH = T.KHH)" ; 
    /**
     * 调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
     */
    --put_line(l_sqlBuf);
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
 
 BEGIN
    --l_tableName := F_IDS_GET_TABLENAME('sparkGpzyResult', I_KHH);
    l_sqlBuf := 
                " SELECT " ||
                "  TZZH_ID     ," ||
                "  SEQNO       ," ||
                "  WTH         ," ||
                "  KHH         ," ||
                "  KHXM        ," ||
                "  GDH         ," ||
                "  JYS         ," ||
                "  BZ          ," ||
                "  YYB         ," ||
                "  ZQDM        ," ||
                "  ZQMC        ," ||
                "  ZQLB        ," ||
                "  JYLB        ," ||
                "  CJBH        ," ||
                "  CJSJ        ," ||
                "  SBSJ        ," ||
                "  CJBS        ," ||
                "  CJSL        ," ||
                "  CJJG        ," ||
                "  JSJ         ," ||
                "  LXJG        ," ||
                "  CJJE        ," ||
                "  LXJE        ," ||
                "  YSJE        ," ||
                "  JSRQ        ," ||
                "  BCZQYE      ," ||
                "  YSSL        ," ||
                "  ZXJ         ," ||
                "  JJJZ        ," ||
                "  SEQNO_QS    ," ||
                "  BCZQYE_QS   ," ||
                "  (CASE WHEN JYLB<>'6' AND (CJSJ_QS='00\:00\:00' OR CJSJ_QS is NULL) THEN '18\:00\:00' ELSE CJSJ_QS END) AS CJSJ_QS     ," ||
                "  CCCB_QS     ," ||
                "  CBJ_QS      ," ||
                "  SXYK_QS     ," ||
                "  LJYK_QS     ," ||
                "  REMARK_QS   ," ||
                "  JYLB_QS     ," ||
                "  (CASE " ||
                "    WHEN JYLB<>'6' AND CJBH IN ('SHFQWT') AND JYLB IN ('43') THEN 0" ||
                "    WHEN JYLB<>'6' AND CJBH IN ('SGFQWT') AND JYLB IN ('42') THEN 0 WHEN (YSSL=0 AND YSJE=0) THEN 0 " ||
                "    ELSE ZT_QS END) AS ZT_QS       ," ||
                "  CAST(NULL AS DECIMAL(16,2)) AS YK_ZC," ||
                "  CAST(NULL AS DECIMAL(16,2)) AS YK_ZR," ||
                "  LSH         ," ||
                "  ROWNUM      ," ||
                "  S1          ," ||
                "  S2          ," ||
                "  S3          ," ||
                "  S4          ," ||
                "  S5          ," ||
                "  S6          ," ||
                "  JYFY,  " ||
                "  CAST(NULL AS DECIMAL(16,2)) AS CCCB_ZC," ||
                "  CAST(NULL AS DECIMAL(16,2)) AS CCCB_ZR," ||
                I_RQ || " as CJRQ" ||
                "  FROM  (SELECT * FROM " || F_IDS_GET_TABLENAME('xy_sparkJgmxlsQsCccb', I_KHH) || "  T where NOT EXISTS (SELECT 1 FROM  " || F_IDS_GET_TABLENAME('xy_sparkJgmxlsXY', I_KHH) || " D WHERE " ||
                "  D.KHH = T.KHH" ||
                " AND D.CJRQ = T.JSRQ" ||
                " AND D.WTH = T.WTH" ||
                " AND D.JYS = T.JYS" ||
                " AND D.ZQDM = T.ZQDM" ||
                " AND D.CJSL = T.CJSL" ||
                " AND D.JYLB = '43'" ||
                " AND D.CJRQ < T.CJRQ" ||
                " AND D.KHH = T.KHH ))"; 
    --F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
 -- 数据插入至资产统计临时表sparkStatDR
    l_sql := 'INSERT INTO TABLE ' || F_IDS_GET_TABLENAME('xy_sparkJgmxlsResult', I_KHH) || l_columns || l_sqlBuf;    
    execute immediate l_sql;
  END;
 
  BEGIN
    /**
     * 写入分区表
     * 入参：临时表名，目标库名，目标表名，目标表分区字段，目标表分区字段值，客户号
     * F_IDS_OVERWRITE_PARTITION(tablename, dbname,targetTable,partitionValue, khh)
     */ 
    F_IDS_OVERWRITE_PARTITION(l_tableName, "CUST", "T_XY_JGMXLS_HIS_QS", I_RQ, I_KHH);
  END; 
end;
/