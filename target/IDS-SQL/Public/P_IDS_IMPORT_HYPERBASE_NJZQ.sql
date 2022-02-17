!set plsqlUseSlash true
CREATE OR REPLACE PROCEDURE cust.p_ids_import_hyperbase (
    I_KSRQ INT,
    I_JSRQ INT
)
IS 
/******************************************************************
  *文件名称：CUST.P_IDS_IMPORT_HYPERBASE
  *项目名称：IDS计算
  *文件说明：iDS数据导入HBASE
 
  创建人： 燕居庆
  功能说明：iDS数据导入HBASE

  参数说明

  修改者        版本号        修改日期        说明
  燕居庆        v1.0.0        2019/06/24       创建
  燕居庆        v1.0.1        2020/08/06       修改：HIS_FP_CPFE，HIS_FP_TZSY增加ZQPZ导入
  杨启旺        v1.0.1        2020/10/30       升级脚本
  ZMH           v1.0.2        2021/10/13       APEX.KHFX_RZD USE LJJZ AS ZXJZ
*******************************************************************/
    V_RQ INT
    TYPE cur_jyr IS REF CURSOR
    V_CUR cur_jyr
BEGIN
    if I_JSRQ IS NULL THEN
        I_JSRQ := I_KSRQ;
    end if;
              ----------------------------------客户信息导入---------------------------------------------------
         BEGIN
              EXECUTE IMMEDIATE "TRUNCATE TABLE apex.base_khxx";

               INSERT INTO apex.base_khxx
               SELECT
                   /*+USE_BULKLOAD*/
                   reverse(KHH) AS rowkey
                    , khh
                    , khxm
                    , khrq
                    , xhrq
                    , dhhm
                    , yyb
                    , fxcsnl
                    , yybmc
               FROM
                   cust.vw_t_khxx
               ORDER BY
                    rowkey;
          END;
          
    OPEN V_CUR FOR  SELECT
                     DISTINCT jyr
                 FROM
                     dsc_cfg.t_xtjyr WHERE jyr BETWEEN I_KSRQ AND I_JSRQ
        LOOP
          
         FETCH V_CUR INTO V_RQ
         EXIT WHEN V_CUR%NOTFOUND;
          ----------------------------------写入交割明细流水表----------------------------------------------
          /*
           * 集中交易债券类别交易单独出去为his_zq_jgmxls
           * 国债逆回购归于理财
           * 信用不拆分
           * 流水号和清算编号不同时为0
           */
          BEGIN
              INSERT INTO apex.his_jy_jgmxls
               SELECT
                   /*+USE_BULKLOAD*/
                   t.*
               FROM
                   (SELECT
                        concat_ws('-',reverse(KHH)
                                , cast(cjrq AS STRING)
                                , '1'
                                , Z.jys
                                , zqdm
                                , gdh
                                , cast(seqno AS STRING)
                                , cast(lsh AS STRING)) AS rowkey
                         , KHH
                         , CJRQ
                         , '1' AS GTLB
                         , Z.JYS
                         , ZQDM
                         , GDH
                         , SEQNO
                         , WTH
                         , KHXM
                         , BZ
                         , YYB
                         , ZQMC
                         , Z.ZQLB
                         , JYLB
                         , CJBH
                         , CJSJ
                         , CJBS
                         , CJSL
                         , CJJG
                         , JSJ
                         , LXJG
                         , CJJE
                         , LXJE
                         , YSJE
                         , JSRQ
                         , BCZQYE
                         , YSSL
                         , ZXJ
                         , JJJZ
                         , S1
                         , S2
                         , S3
                         , S4
                         , S5
                         , S6
                         , JYFY
                         , CCCB_QS
                         , CBJ_QS
                         , SXYK_QS
                         , LJYK_QS
                    FROM
                        CUST.T_JGMXLS_HIS_QS Z
                    WHERE
                         cjrq = V_RQ AND Z.zqlb NOT IN ('H0','H1', 'H3') AND  substr(Z.zqlb,1,1) NOT IN ('Z')--不包含债券以及国债逆回购
                     UNION ALL
                     SELECT
                         concat_ws('-',reverse(KHH)
                                , cast(cjrq AS STRING)
                                , '2'
                                , jys
                                , zqdm
                                , gdh
                                , cast(seqno AS STRING)
                                , cast(lsh AS STRING)) AS rowkey
                          , KHH
                          , CJRQ
                          , '2' AS GTLB
                          , JYS
                          , ZQDM
                          , GDH
                          , SEQNO
                          , WTH
                          , KHXM
                          , BZ
                          , YYB
                          , ZQMC
                          , ZQLB
                          , JYLB
                          , CJBH
                          , CJSJ
                          , CJBS
                          , CJSL
                          , CJJG
                          , JSJ
                          , LXJG
                          , CJJE
                          , LXJE
                          , YSJE
                          , JSRQ
                          , BCZQYE
                          , YSSL
                          , ZXJ
                          , JJJZ
                          , S1
                          , S2
                          , S3
                          , S4
                          , S5
                          , S6
                          , JYFY
                          , CCCB_QS
                          , CBJ_QS
                          , SXYK_QS
                          , LJYK_QS
                     FROM
                         CUST.T_XY_JGMXLS_HIS_QS Z
                     WHERE --不包含国债逆回购
                          cjrq = V_RQ AND Z.zqlb NOT IN ('H0','H1', 'H3') AND  substr(Z.zqlb,1,1) NOT IN ('Z')) t
               ORDER BY
                    rowkey;
          END;
          --------------------------------------写入历史资金流水---------------------------------------------------
          /*
           * 历史资金明细包含 集中/信用/期权
           */
          BEGIN
              INSERT INTO apex.his_jy_zjmxls
               SELECT
                   /*+USE_BULKLOAD*/
                   *
               FROM
                   (SELECT
                        concat_ws('-',reverse(khh)
                                , CAST(rq AS STRING)
                                , '1'
                                , zjzh
                                , ywkm
                                , cast(seqno AS STRING)) AS rowkey
                         , khh
                         , rq
                         , '1' AS gtlb
                         , zjzh
                         , ywkm
                         , seqno
                         , fssj
                         , bz
                         , srje
                         , fcje
                         , bczjye
                         , zy
                         , lsh
                    FROM
                        cust.t_zjmxls_his
                    WHERE
                         rq = V_RQ
                     UNION ALL
                     SELECT
                         concat_ws('-',reverse(khh)
                                 , CAST(rq AS STRING)
                                 , '2'
                                 , zjzh
                                 , ywkm
                                 , cast(seqno AS STRING)) AS rowkey
                          , khh
                          , rq
                          , '2' AS gtlb
                          , zjzh
                          , ywkm
                          , seqno
                          , fssj
                          , bz
                          , srje
                          , fcje
                          , bczjye
                          , zy
                          , lsh
                     FROM
                         cust.t_xy_zjmxls_his
                     WHERE
                          rq = V_RQ
                    UNION ALL
                     SELECT
                         concat_ws('-',reverse(khh)
                                 , CAST(rq AS STRING)
                                 , '5'
                                 , zjzh
                                 , ywkm
                                 , cast(seqno AS STRING)) AS rowkey
                          , khh
                          , rq
                          , '5' AS gtlb
                          , zjzh
                          , ywkm
                          , seqno
                          , fssj
                          , bz
                          , srje
                          , fcje
                          , bczjye
                          , zy
                          , lsh
                     FROM
                         cust.t_so_zjmxls_his
                     WHERE
                          rq = V_RQ) ORDER BY rowkey ;
          END;
          --------------------------------------历史证券余额数据------------------------------------------
          /*
           * 历史证券余额包含集中/信用
           * 另：集中的债券持仓单独存放
           */
          BEGIN
              INSERT INTO apex.his_jy_zqye
               SELECT
                   /*+USE_BULKLOAD*/
                   *
               FROM
                   (SELECT
                        concat_ws('-',reverse(khh)
                                , cast(rq AS STRING)
                                , '1'
                                , z.jys
                                , z.zqdm
                                , gdh) AS rowkey
                         , KHH
                         , RQ
                         , '1' AS GTLB
                         , Z.JYS
                         , Z.ZQDM
                         , GDH
                         , NVL(D.ZQMC, Z.ZQDM) ZQMC
                         , KCRQ
                         , Z.ZQLB
                         , Z.BZ
                         , ZQSL
                         , ZXSZ
                         , CCCB
                         , CBJ
                         , LJYK
                         , TBCCCB
                         , TBCBJ
                         , DRYK
                         , E.SSHY AS GPHY
                         
                    FROM
                        CUST.T_ZQYE_HIS Z
                        LEFT JOIN CUST.T_ZQDM D
                        ON (Z.JYS = D.JYS AND Z.ZQDM = D.ZQDM)
                        LEFT JOIN info.tgp_gsgk E
                        ON (Z.JYS = E.JYS AND Z.ZQDM = E.ZQDM)
                    WHERE
                         Z.rq = V_RQ AND substr(Z.zqlb,1,1) NOT IN ('H', 'Z') --不包含债券
                     UNION ALL
                     SELECT
                         concat_ws('-',reverse(khh)
                                 , cast(rq AS STRING)
                                 , '2'
                                 , z.jys
                                 , z.zqdm
                                 , gdh) AS rowkey
                          , KHH
                          , RQ
                          , '2' AS GTLB
                          , Z.JYS
                          , Z.ZQDM
                          , GDH
                          , NVL(D.ZQMC, Z.ZQDM) ZQMC
                          , KCRQ
                          , Z.ZQLB
                          , Z.BZ
                          , ZQSL
                          , ZXSZ
                          , CCCB
                          , CBJ
                          , LJYK
                          , TBCCCB
                          , TBCBJ
                          , DRYK
                          , E.SSHY AS GPHY
                     FROM
                         CUST.T_XY_ZQYE_HIS Z
                         LEFT JOIN CUST.T_ZQDM D
                         ON (Z.JYS = D.JYS AND Z.ZQDM = D.ZQDM)
                         LEFT JOIN info.tgp_gsgk E
                         ON (Z.JYS = E.JYS AND Z.ZQDM = E.ZQDM)
                     WHERE
                          rq = V_RQ AND substr(Z.zqlb,1,1) NOT IN ('H', 'Z')) ORDER BY rowkey ;
          END;
          ------------------------------------------待交收数据-----------------------------------------------
          /*
           * 交易待交收包含集中/信用
           * 另：集中交易债券代交收单独存放
           */
          BEGIN
              INSERT INTO apex.his_jy_djs
               SELECT
                   /*+USE_BULKLOAD*/
                   *
               FROM
                   (SELECT
                        concat_ws('-',reverse(khh)
                                , cast(T.CJRQ AS STRING)
                                , '1'
                                , T.JYS
                                , T.ZQDM
                                , GDH) AS ROWKEY
                         , T.KHH
                         , T.CJRQ
                         , '1' AS GTLB
                         , T.JYS
                         , T.ZQDM
                         , T.GDH
                         , ROW_NUMBER()
                            OVER
                                 ( PARTITION BY T.KHH
                                   ORDER BY
                                        T.CJRQ
                                        , T.JYS
                                        , T.ZQDM
                                        , T.GDH) AS RN
                         , T.BZ
                         , NVL(D.ZQMC, T.ZQDM) AS ZQMC
                         , T.ZQLB
                         , T.JYLB
                         , T.YSJE
                         , T.SETTLE_DATE
                         , T.SETTLE_DATE_2
                         , T.CJSL
                         , T.CJJE
                         , T.CJJG
                         , T.LXJG
                         , T.LXJE
                         , T.YSRQ
                    FROM
                        CUST.T_DJSQSZL T
                        LEFT JOIN CUST.T_ZQDM D
                        ON (D.JYS = T.JYS AND D.ZQDM = T.ZQDM)
                    WHERE
                         t.cjrq = V_RQ   AND NOT EXISTS (SELECT 1 FROM DSC_CFG.vw_t_zqpz_ids F WHERE f.zqpz = 4 AND t.jys = F.JYS AND t.zqlb=F.ZQLB)
                     UNION ALL
                     SELECT
                         concat_ws('-',reverse(khh)
                                 , cast(T.CJRQ AS STRING)
                                 , '2'
                                 , T.JYS
                                 , T.ZQDM
                                 , GDH) AS ROWKEY
                          , T.KHH
                          , T.CJRQ
                          , '2' AS GTLB
                          , T.JYS
                          , T.ZQDM
                          , T.GDH
                          , ROW_NUMBER()
                             OVER
                                  ( PARTITION BY T.KHH
                                    ORDER BY
                                         T.CJRQ
                                         , T.JYS
                                         , T.ZQDM
                                         , T.GDH) AS RN
                          , T.BZ
                          , NVL(D.ZQMC, T.ZQDM) AS ZQMC
                          , T.ZQLB
                          , T.JYLB
                          , T.YSJE
                          , T.SETTLE_DATE
                          , T.SETTLE_DATE_2
                          , T.CJSL
                          , T.CJJE
                          , T.CJJG
                          , T.LXJG
                          , T.LXJE
                          , T.SETTLE_DATE AS YSRQ
                     FROM
                         CUST.T_XY_DJSQSZL T
                         LEFT JOIN CUST.T_ZQDM D
                         ON (D.JYS = T.JYS AND D.ZQDM = T.ZQDM)
                     WHERE
                          T.cjrq = V_RQ AND NOT EXISTS (SELECT 1 FROM DSC_CFG.vw_t_zqpz_ids F WHERE f.zqpz = 4 AND t.jys = F.JYS AND t.zqlb=F.ZQLB)) ORDER BY rowkey ;
          END;
          --------------------------------------投资损益-------------------------------------------
          /*
           * 投资损益包含集中/信用的投资损益
           * 另：集中交易债券的投资损益单独存放,国债逆回购归于理财产品
           */
          BEGIN
              INSERT INTO apex.his_jy_tzsy
               SELECT
                   /*+USE_BULKLOAD*/
                   *
               FROM
                   (SELECT
                        concat_ws('-',reverse(khh)
                                , cast(qcrq AS STRING)
                                , '1'
                                , T.jys
                                , T.zqdm
                                , T.gdh
                                , cast(kcrq AS STRING)) AS rowkey
                         , T.KHH
                         , T.QCRQ
                         , '1' AS GTLB
                         , T.JYS
                         , T.ZQDM
                         , T.GDH
                         , T.KCRQ
                         , NVL(D.ZQMC, T.ZQDM) AS ZQMC
                         , T.ZQLB
                         , T.BZ
                         , T.MRJE
                         , T.MCJE
                         , T.JYFY
                         , T.LJYK
                         , T.DRYK
                         , E.SSHY AS GPHY
                    FROM
                        CUST.T_TZSY T
                        LEFT JOIN CUST.T_ZQDM D
                        ON (T.JYS = D.JYS AND T.ZQDM = D.ZQDM)
                        LEFT JOIN info.tgp_gsgk E
                        ON (T.JYS = E.JYS AND T.ZQDM = E.ZQDM)
                    WHERE   --国债逆回购归于理财产品
                         T.qcrq = V_RQ  AND substr(T.zqlb,1,1) NOT IN ('H', 'Z') --不包含债券
                     UNION ALL
                     SELECT
                         concat_ws('-',reverse(khh)
                                 , cast(qcrq AS STRING)
                                 , '2'
                                 , T.jys
                                 , T.zqdm
                                 , T.gdh
                                 , cast(kcrq AS STRING)) AS rowkey
                          , T.KHH
                          , T.QCRQ
                          , '2' AS GTLB
                          , T.JYS
                          , T.ZQDM
                          , T.GDH
                          , T.KCRQ
                          , NVL(D.ZQMC, T.ZQDM) AS ZQMC
                          , T.ZQLB
                          , T.BZ
                          , T.MRJE
                          , T.MCJE
                          , T.JYFY
                          , T.LJYK
                          , T.DRYK
                          , E.SSHY AS GPHY
                     FROM
                         CUST.T_XY_TZSY T
                         LEFT JOIN CUST.T_ZQDM D
                         ON (T.JYS = D.JYS AND T.ZQDM = D.ZQDM)
                         LEFT JOIN info.tgp_gsgk E
                         ON (T.JYS = E.JYS AND T.ZQDM = E.ZQDM)
                     WHERE--不包含国债逆回购
                          T.qcrq = V_RQ  AND substr(T.zqlb,1,1) NOT IN ('H', 'Z')) ORDER BY rowkey ;
          END;
        ----------------------------------历史场外/OTC交割流水-----------------------------------
          /*
           * 金融产品交割流水，国债逆回购归于理财，交割也归于理财
           */
          BEGIN
              INSERT INTO apex.his_fp_jgmxls
               SELECT
                   /*+USE_BULKLOAD*/
                   *
               FROM
                   (SELECT
                        concat_ws('-',reverse(khh)
                                , cast(qrrq AS STRING)
                                , jrjgdm
                                , cpdm
                                , cast(case when t.zqpz in ('7205','2510') then 1 else t.app_id end AS STRING)
                                , jrcpzh
                                , cast(seqno AS STRING)
                                , cast(lsh AS STRING)) AS rowkey
                         , T.khh
                         , T.qrrq
                         , T.jrjgdm
                         , T.cpdm
                         , case when t.zqpz in ('7205','2510') then 1 else t.app_id end as app_id
                         , T.jrcpzh
                         , T.seqno
                         , T.lsh
                         , T.khxm
                         , T.jslx
                         , T.jyzh
                         , T.ywdm
                         , T.fqf
                         , T.sqbh
                         , T.cpjc
                         , T.sffs
                         , T.wtfs
                         , T.fsyyb
                         , T.bz
                         , T.jrjglsh
                         , T.qrfe
                         , T.qrje
                         , T.zkl
                         , T.lx
                         , T.sxf
                         , T.dlf
                         , T.zjye
                         , T.feye
                    FROM
                        dsc_bas.t_fp_jgmxls_his T
                    WHERE
                         qrrq = V_RQ) ORDER BY rowkey ;
          END;
          --国债逆回购归于理财
          BEGIN
              INSERT INTO apex.his_fp_jgmxls
               SELECT
                   /*+USE_BULKLOAD*/
                   t.*
               FROM
                   (SELECT
                        concat_ws('-',reverse(KHH)
                                , cast(cjrq AS STRING)
                                , jys
                                , zqdm
                                ,'1'
                                , gdh
                                , cast(seqno AS STRING),
                                cast(lsh AS STRING)) AS rowkey
                         , KHH
                         , CJRQ AS qrrq
                         , JYS
                         , ZQDM
                         ,'1' AS app_id
                         , GDH
                         , SEQNO
                         , WTH
                         , KHXM
                         , NULL AS JSLX
                         , GDH
                         , B.jylbmc AS ywdm
                         , NULL AS FQF
                         , cjbh
                         , ZQMC
                         , NULL AS SFFS
                         , NULL AS WTFS
                         , YYB
                         , BZ
                         , LSH AS JRJGLSH
                         , CJSL AS QRFE
                         , CJJE AS QRJE
                         , NULL AS ZKL
                         , LXJE
                         , JYFY AS SXF
                         , NULL AS DLF
                         , NULL AS ZJYE
                         , BCZQYE
                    FROM
                        CUST.T_JGMXLS_HIS_QS T
                        LEFT JOIN dsc_cfg.t_jylb B
                        ON (T.JYLB = B.jylb)
                    WHERE --国债逆回购归于理财
                         T.cjrq = V_RQ AND T.ZQLB IN ('H0','H1','H3')
                     ) t
               ORDER BY
                    rowkey;
          END;
 ------------------------------------------------导入OTC持仓----------------------------------------------------
            /*
             * 金融产品持仓，包含现金宝
             * 增加现金宝A72001的导入
             */
          BEGIN
              
              INSERT INTO apex.his_fp_cpfe
              SELECT /*+USE_BULKLOAD*/
              *
              FROM (SELECT 
                concat_ws('-', reverse(T.khh), cast(T.rq AS string), T.jrjgdm, T.cpdm, cast(T.app_id AS string), T.jrcpzh) AS rowkey,
                T.KHH  ,
                T.RQ  ,
                T.JRJGDM  ,
                T.CPDM  ,
                T.APP_ID  ,
                T.JRCPZH  ,
                T.CPJC  ,
                T.SFFS  ,
                T.CPSL ,
                T.BZ ,
                T.ZXSZ ,
                T.CPFL ,
                T.KCRQ ,
                T.CCCB ,
                T.LJYK ,
                T.TBCCCB ,
                T.TBCBJ ,
                T.DRYK ,
                T.CBJ,
                (CASE WHEN t.CPDM = 'A72001' THEN '7205' ELSE ZQPZ END ) AS ZQPZ 
                FROM (SELECT * FROM cust.t_fp_cpfe_his where rq=V_RQ) T 
                LEFT JOIN (SELECT * FROM dsc_bas.t_fp_cpfe_his where rq=V_RQ) A ON 
                (T.KHH=A.KHH AND T.APP_ID = A.APP_ID AND T.JRCPZH = A.JRCPZH AND 
                T.JRJGDM = A.JRJGDM AND T.CPDM = A.CPDM AND T.CPSL = A.CPSL 
                AND T.CPFL = A.CPFL AND T.RQ = A.RQ)) ORDER BY rowkey;
                
                
             INSERT INTO apex.his_fp_cpfe
              SELECT /*+USE_BULKLOAD*/
              *
              FROM (
              SELECT 
                concat_ws('-', reverse(T.khh), cast(T.rq AS string), T.JYS, T.ZQDM, cast('1' AS string), cast(T.GDH AS string), cast(T.LSH AS STRING)) AS rowkey,
                T.KHH  ,
                T.RQ  ,
                T.JYS AS JRJGDM  ,
                T.ZQDM AS CPDM  ,
                '1' AS GTLB  ,
                T.GDH  ,
                NVL(D.ZQMC, T.ZQDM) AS CPJC  ,
                cast(NULL AS string) AS SFFS  ,
                T.CJSL AS CPSL ,
                T.BZ ,
                T.YSJE_2 AS ZXSZ ,
                T.ZQLB AS CPFL ,
                T.CJRQ AS KCRQ ,
                ABS(T.YSJE) AS CCCB ,
                T.LXJE AS LJYK ,
                ABS(T.YSJE) AS TBCCCB ,
                T.CJJG AS TBCBJ ,
                0 AS DRYK ,
                T.CJJG AS CBJ,
                 E.JB3_ZQPZ AS ZQPZ
               FROM
                    DSC_BAS.t_djsqszl_his T
                    LEFT JOIN CUST.T_ZQDM D
                    ON (T.JYS = D.JYS AND T.ZQDM = D.ZQDM)
                    LEFT JOIN (SELECT DISTINCT jys,zqlb,JB3_ZQPZ FROM DSC_CFG.T_ZQPZ_DY) E
                    ON (T.JYS = E.JYS AND T.ZQLB = E.ZQLB)
                WHERE   --国债逆回购归于理财产品
                     T.rq = V_RQ AND T.zqlb in ( 'H0','H1', 'H3'))  ORDER BY rowkey;
          END;

------------------------------OTC历史待交收--------------------------------------------------
            /*
             * 金融产品历史待交收
             */
          BEGIN
              INSERT INTO apex.his_fp_djs
                SELECT
                    /*+ USE_BULKLOAD */
                    concat_ws('-'
                               , reverse(khh)
                               , cast(qrrq AS STRING)
                               , t.jrjgdm
                               , t.cpdm
                               , cast(t.app_id AS STRING)
                               , cast(t.jrcpzh AS STRING)
                               , lsh) AS rowkey
                     , t.KHH
                     , t.QRRQ
                     , t.JRJGDM
                     , t.CPDM
                     , t.APP_ID
                     , t.JRCPZH
                     , t.LSH
                     , t.YWDM
                     , NVL(d.CPJC, '') AS cpjc
                     , t.QRJE
                     , t.BZ
                     , t.SETTLE_DATE
                     , t.JSBZ
                     , t.CPFL
                FROM
                    (SELECT
                         *
                     FROM
                         cust.t_fp_djsqszl_his t
                     WHERE
                          qrrq = V_RQ) t
                    LEFT JOIN cust.t_jrcpdm d
                    ON (t.app_id = d.app_id
                         AND t.jrjgdm = d.jrjgdm
                         AND t.cpdm = d.cpdm) ORDER BY rowkey;
           END;

---------------------------------OTC清仓-----------------------------------------
           /*
            * 金融产品清仓
            */
           BEGIN
               INSERT INTO apex.his_fp_tzsy
               SELECT /*+USE_BULKLOAD*/
                concat_ws('-', reverse(khh), cast(qcrq AS STRING), t.jrjgdm, t.cpdm, cast(t.app_id AS STRING), t.jrcpzh, cast(t.kcrq AS string)) AS rowkey,
                t.KHH  ,
                t.QCRQ  ,
                t.JRJGDM  ,
                t.CPDM  ,
                t.APP_ID  ,
                t.JRCPZH  ,
                t.KCRQ  ,
                NVL(d.CPJC, '') AS CPJC,
                t.CPFL ,
                t.BZ ,
                t.MRJE ,
                t.MCJE ,
                t.JYFY ,
                t.LJYK ,
                t.DRYK ,
                cast(CASE WHEN d.cplb IN ('607') THEN
                    '7205'
                ELSE
                 d.cplb
                 END AS STRING) AS  ZQPZ
                from (SELECT* FROM cust.t_fp_tzsy WHERE qcrq = V_RQ) t 
                left join cust.t_jrcpdm d on (t.app_id=d.app_id and t.jrjgdm=d.jrjgdm and t.cpdm=d.cpdm) ORDER BY rowkey;
                
                --国债逆回购属于理财
               delete from apex.his_fp_tzsy where qcrq=V_RQ and APP_ID='1';
               INSERT INTO apex.his_fp_tzsy
               SELECT 
                        concat_ws('-',reverse(khh)
                                , cast(qcrq AS STRING)
                                , T.jys
                                , T.zqdm
                                , '1'
                                , T.gdh
                                , cast(kcrq AS STRING)
                                , cast(row_number() over() AS STRING)) AS rowkey
                         , T.KHH
                         , T.QCRQ
                         , T.JYS
                         , T.ZQDM
                         , '1' AS GTLB
                         , T.GDH
                         , T.KCRQ
                         , NVL(D.ZQMC, T.ZQDM) AS ZQMC
                         , T.ZQLB
                         , T.BZ
                         , T.MRJE
                         , T.MCJE
                         , T.JYFY
                         , T.LJYK
                         , T.DRYK
                         , E.JB3_ZQPZ AS ZQPZ
                    FROM
                        CUST.T_TZSY T
                        LEFT JOIN CUST.T_ZQDM D
                        ON (T.JYS = D.JYS AND T.ZQDM = D.ZQDM)
                        LEFT JOIN (SELECT DISTINCT jys,zqlb,JB3_ZQPZ FROM DSC_CFG.T_ZQPZ_DY) E
                        ON (T.JYS = E.JYS AND T.ZQLB = E.ZQLB)
                    WHERE   --国债逆回购归于理财产品
                         T.qcrq = V_RQ AND T.zqlb in ( 'H0','H1', 'H3')  ORDER BY rowkey;
           END;
/*-------------------------------两融负债变动明细----------------------------------------
           BEGIN
               --DELETE FROM apex.his_xy_fzbdmx WHERE rowkey LIKE '%-' || V_RQ || '-%';
               INSERT INTO apex.his_xy_fzbdmx
               SELECT /*+USE_BULKLOAD*/
                /*concat_ws('-',reverse(khh), cast(rq AS STRING), cast(jys AS STRING), zqdm, cast(jylb AS STRING), cast(wth AS STRING)) AS rowkey,
                KHH  ,
                RQ  ,
                JYS  ,
                ZQDM  ,
                JYLB  ,
                WTH  ,
                YYB ,
                FSRQ ,
                ZQMC ,
                ZQLB ,
                RZSL ,
                RZJE ,
                RQSL ,
                RQJE ,
                FZBJ ,
                HKJE ,
                FZSL ,
                HQSL ,
                RZFY ,
                RQFY ,
                BDRQ ,
                YJLX ,
                GHLX ,
                ZXSZ ,
                YSJE_RQMC ,
                XZRQFZ ,
                XZRQYJLX ,
                XZRZYJLX ,
                FDYK ,
                DRYK ,
                XZHKJE ,
                XZRZHKJE ,
                XZRQHKJE ,
                FZZT ,
                XZGHLX ,
                XZRZGHLX ,
                XZRQGHLX 
                FROM cust.t_xy_fzxxbdmx_his WHERE rq = V_RQ ORDER BY rowkey;
           END;*/
-----------------------------期权历史交割流水--------------------------------------
           BEGIN
               --DELETE FROM apex.his_so_jgmxls WHERE rowkey LIKE '%-' || V_RQ || '-%';
               INSERT INTO apex.his_so_jgmxls
               SELECT /*+USE_BULKLOAD*/
                concat_ws('-', reverse(khh),cast(cjrq AS STRING), CAST(t.jys AS STRING), t.hydm, gdh, t.zzhbm, cast(seqno AS STRING), cast(wth AS STRING)) AS rowkey,
                t.KHH  ,
                t.CJRQ ,
                t.JYS  ,
                t.HYDM ,
                t.GDH  ,
                t.ZZHBM,
                t.SEQNO,
                t.WTH  ,
                t.KHXM ,
                t.BZ   ,
                t.YYB  ,
                t.HYMC ,
                t.HYDW ,
                t.ZQLX ,
                NVL(d.ZQDM,'') AS ZQDM ,
                t.QQLX ,
                t.CJBH ,
                t.CJSJ ,
                t.CJBS ,
                t.CJSL ,
                t.CJJG ,
                t.JSJ  ,
                t.CJJE ,
                t.YSJE ,
                t.JSRQ ,
                t.BCYE ,
                t.YSSL ,
                t.BCSL ,
                t.S1 ,
                t.S2 ,
                t.S3 ,
                t.S4 ,
                t.S5 ,
                t.S6
                FROM (SELECT * FROM cust.t_so_jgmxls_his WHERE cjrq = V_RQ) t LEFT JOIN cust.t_so_hydm d ON t.hydm = d.hydm AND t.jys = d.jys ORDER BY rowkey;
           END;
-----------------------------期权历史持仓------------------------------------------
           BEGIN
               --DELETE FROM apex.his_so_zqye WHERE rowkey LIKE '%-' || V_RQ || '-%';
               INSERT INTO apex.his_so_zqye
               SELECT /*+USE_BULKLOAD*/
                concat_ws('-', reverse(khh), cast(rq AS STRING), cast(jys AS STRING), hydm, ccfx, cast(gdh AS STRING), zzhbm) AS rowkey,
                KHH    ,
                RQ     ,
                JYS    ,
                HYDM   ,
                CCFX   ,
                GDH    ,
                ZZHBM  ,
                HYMC  ,
                KCRQ  ,
                QQLX  ,
                BZ    ,
                ZQSL  ,
                ZXSZ  ,
                KCSL  ,
                KCJE  ,
                PCSL  ,
                PCJE  ,
                CCCB  ,
                CBJ   ,
                LJYK  ,
                TBCCCB,
                TBCBJ ,
                DRYK
                FROM cust.t_so_zqye_his WHERE rq = V_RQ ORDER BY rowkey;
           END;
-----------------------------期权历史 清仓------------------------------------------
           BEGIN
               --DELETE FROM apex.his_so_tzsy WHERE rowkey LIKE '%-' || V_RQ || '-%';
               INSERT INTO apex.his_so_tzsy
               SELECT /*+USE_BULKLOAD*/
                concat_ws('-', reverse(khh), cast(t.qcrq AS STRING), cast(t.hydm AS string), cast(t.ccfx AS STRING), cast(t.gdh AS STRING), t.zzhbm ) AS rowkey,
                KHH    ,
                t.QCRQ   ,
                t.CCFX   ,
                t.GDH    ,
                t.ZZHBM  ,
                t.KCRQ  ,
                t.JYS   ,
                t.HYDM  ,
                NVL(d.HYMC, d.hydm) AS HYMC  ,
                t.QQLX  ,
                t.BZ    ,
                t.KCJE  ,
                t.PCJE  ,
                t.JYFY  ,
                t.LJYK  ,
                t.DRYK
                FROM (SELECT * FROM cust.t_so_tzsy WHERE qcrq = V_RQ) t LEFT JOIN cust.t_so_hydm d ON t.hydm = d.hydm AND t.jys = d.jys ORDER BY rowkey;
           END;

-----------------------------指数行情----------------------------------------------
           BEGIN
               --DELETE FROM apex.info_zshq WHERE rowkey LIKE '%-' || V_RQ;
               INSERT INTO apex.info_zshq
               SELECT /*+USE_BULKLOAD*/
                concat_ws('-', zsdm, cast(rq AS STRING),jys) AS rowkey,
                ZSDM  ,
                JYS  ,
                RQ  ,
                ZSMC ,
                ZXJ ,
                ZSP ,
                JKP ,
                ZGJ ,
                ZDJ ,
                CJSL ,
                CJJE 
               FROM info.this_zshq WHERE rq = V_RQ ORDER BY rowkey;
           END;
----------------------------全账户日资产--------------------------------------------
           BEGIN
               --DELETE FROM apex.khfx_rzd WHERE rowkey LIKE '%-' || V_RQ;
               INSERT INTO apex.khfx_rzd
               SELECT /*+USE_BULKLOAD*/
               concat_ws('-', reverse(khh), cast(rq AS string)) AS rowkey,
               KHH               ,
                RQ                ,
                ZZC              ,
                ZJYE             ,
                ZQSZ             ,
                ZFZ              ,
                YK               ,
                YKL              ,
                YK_BY            ,
                YK_BN            ,
                CRJE             ,
                QCJE             ,
                ZRZQSZ           ,
                ZCZQSZ           ,
                ZCJLR            ,
                ZXFE             ,
                LJJZ AS ZXJZ     ,
                ZXJZ_ZZL         ,
                ZSHQ_HS300       ,
                ZSHQ_HS300_ZZL   ,
                ZCFB_ZQPZ_LIST   ,
                ZCFB_ZCLB_LIST   ,
                YL_ZQPZ_LIST     ,
                KS_ZQPZ_LIST     ,
                ZZC_JZJY         ,
                ZQSZ_JZJY        ,
                ZJYE_JZJY        ,
                YK_JZJY          ,
                FDYK_JZJY        ,
                cast(ZFZ-ZFZ_RZRQ AS STRING) AS ZFZ_JZJY         ,
                ZXJZ_JZJY        ,
                ZXJZ_ZZL_JZJY    ,
                ZZC_RZRQ         ,
                ZQSZ_RZRQ        ,
                ZJYE_RZRQ        ,
                ZFZ_RZRQ         ,
                YK_RZRQ          ,
                FDYK_RZRQ        ,
                ZXJZ_RZRQ        ,
                ZXJZ_ZZL_RZRQ    ,
                ZQSZ_JRCP        ,
                YK_JRCP          ,
                FDYK_JRCP        ,
                ZZC_GGQQ         ,
                ZJYE_GGQQ        ,
                ZQSZ_GGQQ        ,
                YK_GGQQ          ,
                FDYK_GGQQ        ,
                ZXJZ_GGQQ        ,
                ZXJZ_ZZL_GGQQ    ,
                QTSR,
                QTZC
                FROM cust.t_stat_zd_r WHERE rq = V_RQ ORDER BY rowkey;
           END;
           --------------------------------------历史债券持仓数据20190807------------------------------------------
           /*
            * 债券持仓来自集中交易的债券持仓
            */
          BEGIN
              INSERT INTO apex.his_zq_zqye
               SELECT
                   /*+USE_BULKLOAD*/
                   *
               FROM
                   (SELECT
                        concat_ws('-',reverse(khh)
                                , cast(rq AS STRING)
                                , '1'
                                , z.jys
                                , z.zqdm
                                , gdh) AS rowkey
                         , KHH
                         , RQ
                         , '1' AS GTLB
                         , Z.JYS
                         , Z.ZQDM
                         , GDH
                         , NVL(D.ZQMC, Z.ZQDM) ZQMC
                         , KCRQ
                         , Z.ZQLB
                         , Z.BZ
                         , ZQSL
                         , ZXSZ
                         , CCCB
                         , CBJ
                         , LJYK
                         , TBCCCB
                         , TBCBJ
                         , DRYK
                         , E.SSHY AS GPHY
                    FROM
                        CUST.T_ZQYE_HIS Z
                        LEFT JOIN CUST.T_ZQDM D
                        ON (Z.JYS = D.JYS AND Z.ZQDM = D.ZQDM)
                        LEFT JOIN info.tgp_gsgk E
                        ON (Z.JYS = E.JYS AND Z.ZQDM = E.ZQDM)
                    WHERE
                         rq = V_RQ  AND substr(Z.zqlb,1,1) = 'Z' 
                    UNION ALL
                     SELECT
                         concat_ws('-',reverse(khh)
                                 , cast(rq AS STRING)
                                 , '2'
                                 , z.jys
                                 , z.zqdm
                                 , gdh) AS rowkey
                          , KHH
                          , RQ
                          , '2' AS GTLB
                          , Z.JYS
                          , Z.ZQDM
                          , GDH
                          , NVL(D.ZQMC, Z.ZQDM) ZQMC
                          , KCRQ
                          , Z.ZQLB
                          , Z.BZ
                          , ZQSL
                          , ZXSZ
                          , CCCB
                          , CBJ
                          , LJYK
                          , TBCCCB
                          , TBCBJ
                          , DRYK
                          , E.SSHY AS GPHY
                     FROM
                         CUST.T_XY_ZQYE_HIS Z
                         LEFT JOIN CUST.T_ZQDM D
                         ON (Z.JYS = D.JYS AND Z.ZQDM = D.ZQDM)
                         LEFT JOIN info.tgp_gsgk E
                         ON (Z.JYS = E.JYS AND Z.ZQDM = E.ZQDM)
                     WHERE
                          rq = V_RQ AND substr(Z.zqlb,1,1) = 'Z' 
                     ) ORDER BY rowkey ;
          END;
          
         --------------------------------------债券投资损益-------------------------------------------
         /*
          * 债券投资损益来自集中交易的债券投资损益
          */
          BEGIN
              INSERT INTO apex.his_zq_tzsy
               SELECT
                   /*+USE_BULKLOAD*/
                   *
               FROM
                   (SELECT
                        concat_ws('-',reverse(khh)
                                , cast(qcrq AS STRING)
                                , '1'
                                , T.jys
                                , T.zqdm
                                , T.gdh
                                , cast(kcrq AS STRING)) AS rowkey
                         , T.KHH
                         , T.QCRQ
                         , '1' AS GTLB
                         , T.JYS
                         , T.ZQDM
                         , T.GDH
                         , T.KCRQ
                         , NVL(D.ZQMC, T.ZQDM) AS ZQMC
                         , T.ZQLB
                         , T.BZ
                         , T.MRJE
                         , T.MCJE
                         , T.JYFY
                         , T.LJYK
                         , T.DRYK
                         , E.SSHY AS GPHY
                    FROM
                        CUST.T_TZSY T
                        LEFT JOIN CUST.T_ZQDM D
                        ON (T.JYS = D.JYS AND T.ZQDM = D.ZQDM)
                        LEFT JOIN info.tgp_gsgk E
                        ON (T.JYS = E.JYS AND T.ZQDM = E.ZQDM)
                    WHERE
                         T.qcrq = V_RQ  AND  substr(T.zqlb,1,1) = 'Z'  AND NOT EXISTS (SELECT 1 FROM info.tzq_kzz WHERE sgdm = T.ZQDM) --去除可转债申购代码
                    UNION ALL
                    SELECT
                         concat_ws('-',reverse(khh)
                                 , cast(qcrq AS STRING)
                                 , '2'
                                 , T.jys
                                 , T.zqdm
                                 , T.gdh
                                 , cast(kcrq AS STRING)) AS rowkey
                          , T.KHH
                          , T.QCRQ
                          , '2' AS GTLB
                          , T.JYS
                          , T.ZQDM
                          , T.GDH
                          , T.KCRQ
                          , NVL(D.ZQMC, T.ZQDM) AS ZQMC
                          , T.ZQLB
                          , T.BZ
                          , T.MRJE
                          , T.MCJE
                          , T.JYFY
                          , T.LJYK
                          , T.DRYK
                          , E.SSHY AS GPHY
                     FROM
                         CUST.T_XY_TZSY T
                         LEFT JOIN CUST.T_ZQDM D
                         ON (T.JYS = D.JYS AND T.ZQDM = D.ZQDM)
                         LEFT JOIN info.tgp_gsgk E
                         ON (T.JYS = E.JYS AND T.ZQDM = E.ZQDM)
                     WHERE--不包含国债逆回购
                          T.qcrq = V_RQ AND  substr(T.zqlb,1,1) = 'Z'  AND NOT EXISTS (SELECT 1 FROM info.tzq_kzz WHERE sgdm = T.ZQDM)
                         ) ORDER BY rowkey ;
          END;
          
                    ----------------------------------写入债券交割明细流水表----------------------------------------------
        /*
         * 债券交割明细流水来自集中交易的债券交割明细
         */
          BEGIN
              INSERT INTO apex.his_zq_jgmxls
               SELECT
                   /*+USE_BULKLOAD*/
                   t.*
               FROM
                   (SELECT
                        concat_ws('-',reverse(KHH)
                                , cast(cjrq AS STRING)
                                , '1'
                                , jys
                                , zqdm
                                , gdh
                                , cast(seqno AS STRING)
                                , cast(lsh AS STRING)) AS rowkey
                         , KHH
                         , CJRQ
                         , '1' AS GTLB
                         , JYS
                         , ZQDM
                         , GDH
                         , SEQNO
                         , WTH
                         , KHXM
                         , BZ
                         , YYB
                         , ZQMC
                         , ZQLB
                         , JYLB
                         , CJBH
                         , CJSJ
                         , CJBS
                         , CJSL
                         , CJJG
                         , JSJ
                         , LXJG
                         , CJJE
                         , LXJE
                         , YSJE
                         , JSRQ
                         , BCZQYE
                         , YSSL
                         , ZXJ
                         , JJJZ
                         , S1
                         , S2
                         , S3
                         , S4
                         , S5
                         , S6
                         , JYFY
                         , CCCB_QS
                         , CBJ_QS
                         , SXYK_QS
                         , LJYK_QS
                    FROM
                        CUST.T_JGMXLS_HIS_QS T
                        
                    WHERE --债券类别Z，计入债券
                         T.cjrq = V_RQ AND  substr(T.zqlb,1,1) = 'Z' 
                    union all
                    SELECT
                        concat_ws('-',reverse(KHH)
                                , cast(cjrq AS STRING)
                                , '2'
                                , jys
                                , zqdm
                                , gdh
                                , cast(seqno AS STRING)
                                , cast(lsh AS STRING)) AS rowkey
                         , KHH
                         , CJRQ
                         , '2' AS GTLB
                         , JYS
                         , ZQDM
                         , GDH
                         , SEQNO
                         , WTH
                         , KHXM
                         , BZ
                         , YYB
                         , ZQMC
                         , ZQLB
                         , JYLB
                         , CJBH
                         , CJSJ
                         , CJBS
                         , CJSL
                         , CJJG
                         , JSJ
                         , LXJG
                         , CJJE
                         , LXJE
                         , YSJE
                         , JSRQ
                         , BCZQYE
                         , YSSL
                         , ZXJ
                         , JJJZ
                         , S1
                         , S2
                         , S3
                         , S4
                         , S5
                         , S6
                         , JYFY
                         , CCCB_QS
                         , CBJ_QS
                         , SXYK_QS
                         , LJYK_QS
                    FROM
                        CUST.T_XY_JGMXLS_HIS_QS T
                        
                    WHERE --债券类别Z，计入债券
                         T.cjrq = V_RQ AND  substr(T.zqlb,1,1) = 'Z') t
               ORDER BY
                    rowkey;
          END;
     END LOOP;
    CLOSE V_CUR;
END;
