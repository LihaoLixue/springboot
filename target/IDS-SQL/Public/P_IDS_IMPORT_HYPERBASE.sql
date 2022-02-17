!set plsqlUseSlash true
CREATE OR REPLACE PROCEDURE cust.p_ids_import_hyperbase (
    I_KSRQ INT,
    I_JSRQ INT
)
IS 
    V_RQ INT
    TYPE cur_jyr IS REF CURSOR
    V_CUR cur_jyr
BEGIN
    if I_JSRQ IS NULL THEN
        I_JSRQ := I_KSRQ;
    end if;
    OPEN V_CUR FOR  SELECT
                     DISTINCT jyr
                 FROM
                     dsc_cfg.t_xtjyr WHERE jyr BETWEEN I_KSRQ AND I_JSRQ
        LOOP
          
         FETCH V_CUR INTO V_RQ
         EXIT WHEN V_CUR%NOTFOUND;
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
          ----------------------------------写入交割明细流水表----------------------------------------------
          BEGIN
              --TRUNCATE TABLE apex.his_jy_jgmxls;
              INSERT INTO apex.his_jy_jgmxls
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
                                , cast(seqno AS STRING)) AS rowkey
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
                        CUST.T_JGMXLS_HIS_QS
                    WHERE
                         cjrq = V_RQ
                     UNION ALL
                     SELECT
                         concat_ws('-',reverse(KHH)
                                , cast(cjrq AS STRING)
                                , '2'
                                , jys
                                , zqdm
                                , gdh
                                , cast(seqno AS STRING)) AS rowkey
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
                         CUST.T_XY_JGMXLS_HIS_QS
                     WHERE
                          cjrq = V_RQ) t
               ORDER BY
                    rowkey;
          END;
          --------------------------------------写入历史资金流水---------------------------------------------------
          BEGIN
             --DELETE apex.his_jy_zjmxls WHERE rq =  V_RQ;
             --TRUNCATE TABLE apex.his_jy_zjmxls;
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
                          rq = V_RQ) ORDER BY rowkey ;
          END;
          --------------------------------------历史证券余额数据------------------------------------------
          BEGIN
          --SHOW CREATE TABLE apex.his_jy_zqye;
              --TRUNCATE TABLE apex.his_jy_zqye;
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
                         rq = V_RQ
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
                          rq = V_RQ) ORDER BY rowkey ;
          END;
          ------------------------------------------待交收数据-----------------------------------------------
          BEGIN
          --SHOW CREATE TABLE apex.his_jy_djs;
              --TRUNCATE TABLE apex.his_jy_djs;
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
                         t.cjrq = V_RQ
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
                          T.cjrq = V_RQ) ORDER BY rowkey ;
          END;
          --------------------------------------投资损益-------------------------------------------
          BEGIN
          --SHOW CREATE TABLE apex.his_jy_tzsy;
              --TRUNCATE TABLE apex.his_jy_tzsy;
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
                    WHERE
                         T.qcrq = V_RQ
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
                     WHERE
                          T.qcrq = V_RQ) ORDER BY rowkey ;
          END;
          ----------------------------------历史场外/OTC交割流水-----------------------------------
          BEGIN
          --SHOW CREATE TABLE apex.his_fp_jgmxls;
              --TRUNCATE TABLE apex.his_fp_jgmxls;
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
                                , cast(app_id AS STRING)
                                , jrcpzh
                                , cast(seqno AS STRING)
                                , cast(lsh AS STRING)) AS rowkey
                         , T.khh
                         , T.qrrq
                         , T.jrjgdm
                         , T.cpdm
                         , T.app_id
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
                        cust.t_fp_jgmxls_his T
                    WHERE
                         qrrq = V_RQ) ORDER BY rowkey ;
          END;
 ------------------------------------------------导入OTC持仓----------------------------------------------------       
          BEGIN
              EXECUTE IMMEDIATE "TRUNCATE TABLE apex.his_fp_cpfe";
              INSERT INTO apex.his_fp_cpfe
              SELECT /*+USE_BULKLOAD*/
              *
              FROM (SELECT 
                concat_ws('-', reverse(khh), cast(rq AS string), jrjgdm, cpdm, cast(app_id AS string), jrcpzh) AS rowkey,
                KHH  ,
                RQ  ,
                JRJGDM  ,
                CPDM  ,
                APP_ID  ,
                JRCPZH  ,
                CPJC  ,
                SFFS  ,
                CPSL ,
                BZ ,
                ZXSZ ,
                CPFL ,
                KCRQ ,
                CCCB ,
                LJYK ,
                TBCCCB ,
                TBCBJ ,
                DRYK ,
                CBJ FROM cust.t_fp_cpfe_his WHERE rq = V_RQ) ORDER BY rowkey;
          END;

------------------------------OTC历史待交收--------------------------------------------------
          BEGIN
              EXECUTE IMMEDIATE "TRUNCATE TABLE APEX.HIS_FP_DJS" ;
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
           BEGIN
               EXECUTE IMMEDIATE "TRUNCATE TABLE apex.his_fp_tzsy";
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
                t.DRYK 
                from (SELECT* FROM cust.t_fp_tzsy WHERE qcrq = V_RQ) t 
                left join cust.t_jrcpdm d on (t.app_id=d.app_id and t.jrjgdm=d.jrjgdm and t.cpdm=d.cpdm) ORDER BY rowkey;
           END;
-------------------------------两融负债变动明细----------------------------------------
           BEGIN
               INSERT INTO apex.his_xy_fzbdmx
               SELECT /*+USE_BULKLOAD*/
                concat_ws('-',reverse(khh), cast(rq AS STRING), cast(jys AS STRING), zqdm, cast(jylb AS STRING), cast(wth AS STRING)) AS rowkey,
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
           END;
-----------------------------期权历史交割流水--------------------------------------
           BEGIN
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
               INSERT INTO apex.his_so_tzsy
               SELECT /*+USE_BULKLOAD*/
                concat_ws('-', reverse(khh), cast(t.qcrq AS STRING), cast(t.ccfx AS STRING), cast(t.gdh AS STRING), t.zzhbm ) AS rowkey,
                KHH    ,
                t.QCRQ   ,
                t.CCFX   ,
                t.GDH    ,
                t.ZZHBM  ,
                t.KCRQ  ,
                t.JYS   ,
                t.HYDM  ,
                NVL(d.HYMC, '') AS HYMC  ,
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
               INSERT INTO apex.info_zshq
               SELECT /*+USE_BULKLOAD*/
                concat_ws('-', zsdm,jys, cast(rq AS STRING)) AS rowkey,
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
                ZXJZ             ,
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
                cast(0 AS STRING) AS ZFZ_JZJY         ,
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
                ZXJZ_ZZL_GGQQ  
                FROM cust.t_stat_zd_r WHERE rq = V_RQ ORDER BY rowkey;
           END;
----------------------------全账户月账单-------------------------------------------
           BEGIN
               DELETE FROM apex.khfx_yzd WHERE yf = substr(V_RQ, 1, 6);
               INSERT INTO apex.khfx_yzd
               SELECT /*+USE_BULKLOAD*/
                concat_ws('-', reverse(khh), cast(yf AS string)) AS rowkey,
                KHH   ,
                YF    ,
                QMZZC ,
                QCZZC ,
                QMZJYE,
                QMZQSZ,
                QMZFZ ,
                YK    ,
                YKL   ,
                NHSYL ,
                CRJE  ,
                QCJE  ,
                ZRZQSZ,
                ZCZQSZ,
                ZCJLR ,
                BDL   ,
                ZDHCL ,
                YKL_PM,
                YKL_PM_RATIO,
                CW_GP ,
                HSL_GP,
                YK_GP ,
                CGGS_GP   ,
                CGCGL_GP  ,
                CJJE_GP_MR,
                CJJE_GP_MC,
                JYBS_GP_MR,
                JYBS_GP_MC,
                ZTCS_GP   ,
                ZTSL_GP   ,
                ZTCGL_GP  ,
                YL_LIST   ,
                KS_LIST   ,
                YL_ZQPZ_LIST   ,
                KS_ZQPZ_LIST   ,
                SY_RANK   ,
                ZTCGL_RANK,
                ZDHCL_RANK,
                XGCGL_RANK,
                TZGJZ ,
                QMZZC_JZJY,
                QCZZC_JZJY,
                QMZQSZ_JZJY    ,
                QMZJYE_JZJY    ,
                YK_JZJY   ,
                QMZXJZ_JZJY    ,
                ZXJZ_ZZL_JZJY  ,
                QMZZC_RZRQ,
                QCZZC_RZRQ,
                QMZQSZ_RZRQ    ,
                QMZJYE_RZRQ    ,
                QMZFZ_RZRQ,
                YK_RZRQ   ,
                QMZXJZ_RZRQ    ,
                ZXJZ_ZZL_RZRQ,
                QMZQSZ_JRCP  ,
                YK_JRCP   ,
                QMZZC_GGQQ
                FROM cust.t_stat_zd_y WHERE yf = substr(V_RQ,1,6) ORDER BY rowkey;
           END;
----------------------------全账户年账单-------------------------------------------
           BEGIN
               DELETE FROM apex.khfx_nzd WHERE nf = substr(V_RQ, 1, 4);
               INSERT INTO apex.khfx_nzd
               SELECT /*+USE_BULKLOAD*/
                concat_ws('-', reverse(khh), cast(nf AS string)) AS rowkey,
                KHH            ,
                NF             ,
                QMZZC          ,
                QCZZC          ,
                QMZJYE         ,
                QMZQSZ         ,
                QMZFZ          ,
                YK             ,
                YKL            ,
                NHSYL          ,
                CRJE           ,
                QCJE           ,
                ZRZQSZ         ,
                ZCZQSZ         ,
                ZCJLR          ,
                BDL            ,
                ZDHCL          ,
                YKL_PM         ,
                YKL_PM_RATIO   ,
                CW_GP          ,
                HSL_GP         ,
                YK_GP          ,
                CGGS_GP        ,
                CGCGL_GP       ,
                CJJE_GP_MR     ,
                CJJE_GP_MC     ,
                JYBS_GP_MR     ,
                JYBS_GP_MC     ,
                ZTCS_GP        ,
                ZTSL_GP        ,
                ZTCGL_GP       ,
                YL_LIST        ,
                KS_LIST        ,
                YL_ZQPZ_LIST   ,
                KS_ZQPZ_LIST   ,
                SY_RANK        ,
                ZTCGL_RANK     ,
                ZDHCL_RANK     ,
                XGCGL_RANK     ,
                TZGJZ          ,
                YKZJ           ,
                QMZZC_JZJY     ,
                QCZZC_JZJY     ,
                QMZQSZ_JZJY    ,
                QMZJYE_JZJY    ,
                YK_JZJY        ,
                QMZXJZ_JZJY    ,
                ZXJZ_ZZL_JZJY  ,
                QMZZC_RZRQ     ,
                QCZZC_RZRQ     ,
                QMZQSZ_RZRQ    ,
                QMZJYE_RZRQ    ,
                QMZFZ_RZRQ     ,
                YK_RZRQ        ,
                QMZXJZ_RZRQ    ,
                ZXJZ_ZZL_RZRQ  ,
                QMZQSZ_JRCP    ,
                YK_JRCP        ,
                QMZZC_GGQQ 
                FROM cust.t_stat_zd_n WHERE nf= substr(V_RQ, 1, 4) ORDER BY rowkey;
           END;
---------------------客户投资诊断----------------------------------
          /*BEGIN
               INSERT INTO apex.khfx_tznl
               SELECT /*+USE_BULKLOAD*/
                /*concat_ws('-', reverse(khh), cast(sj AS string)) AS rowkey,
                KHH  ,
                SJ  ,
                ZHPF ,
                BEAT_PER ,
                SYL ,
                SHARP ,
                CALMA ,
                BETA ,
                PMNL_PF ,
                PMNL_PJ ,
                PMNL ,
                PMNL_ZS ,
                ZSHQ_ZZL ,
                SYL_DB_ZS ,
                YLNL_PF ,
                YLNL_PJ ,
                FKNL_PF ,
                FKNL_PJ ,
                XGNL_PF ,
                XGNL_PJ ,
                CGCGL_GP ,
                CGGS_GP ,
                YL_GP ,
                KS_GP ,
                YKB ,
                ZTCGL_GP ,
                ZSCGL ,
                ZSNLPF ,
                ZSNLPJ 
                FROM 
           END;*/
     END LOOP;
    CLOSE V_CUR;
END;
/