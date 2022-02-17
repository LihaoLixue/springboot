!set plsqlUseSlash true
CREATE OR REPLACE PROCEDURE cust.p_ids_xy_init_cacl_zsjz(
                            i_rq in int
)is
/*********************************************************************************************
    *文件名称：CUST.P_IDS_XY_INIT_CACL
    *项目名称：IDS计算
    *文件说明：清算初始化

    创建人：王睿驹
    功能说明：清算初始化
    
    修改者            版本号            修改日期            说明
    王睿驹            v1.0.0            2019/6/17            创建
    王睿驹            v1.0.1            2019/9/16            根据java代码改动修改
    燕居庆            v1.0.2            2019/11/15           新增其他收入、其他支出
    燕居庆            v1.0.3            2019/12/02           修改：持仓成本、累计盈亏来自柜台持仓成本、累计盈亏
--------------------------------------------------------------------------------------------------------------
	邱建锋			  v2.0.1		    2021/1/5   			 修改为净值折算版本，表结构有所调整	
	钟梦涵            v2.0.2            2021/11/04           调整客户的过滤条件
*********************************************************************************************/
DECLARE 
    l_hlcsHKD DECIMAL(12,6);--港币汇率
    l_hlcsUSD DECIMAL(12,6);--美元汇率
    l_dbname STRING;--表前缀
    l_suffix STRING;--表后缀
    l_sqlBuf STRING;
    l_sqlWhere STRING;
    l_sqlWhereCjrq STRING;
    l_tableName_sparkJgmxlsQS STRING;
    l_tableName_sparkFzxxbdmx STRING;
    l_tableName_sparkZqhqHisToday STRING;--证券行情
    l_tableName_sparkZqlb STRING;--证券类别
    l_tableName_sparkFzxx STRING;--融资融券负债数据
    l_tableName_sparkZqyeGT STRING;
    l_tableName_sparkZqyeGTResult STRING;--持仓
    l_tableName_sparkZqyeCbjsGT STRING;--成本
    l_tableName_sparkzcxz STRING;--初始化
    l_tableName_sparkzcZjye STRING;--1、生成资金余额数据
    l_tableName_sparkzcZqsz STRING;--2、生成证券市值数据
    l_tableName_sparkzcFzxx STRING;--3、融资融券负债数据
    l_tableName_sparkzcZcxz STRING;--4、处理在途资产等修正数据
    l_tableName_sparkStatDR STRING;--数据集合
    l_tableName_sparkzcFZxxbdmx STRING;--
    l_tableName_sparkZctjResult STRING;
	l_tableName STRING;
BEGIN
    CUST.P_IDS_XY_DEBT_CHECK(i_rq,NULL); --负债明细
    
    l_dbname := "tempspark.";
    SELECT CAST(F_GET_HLCS('2',i_rq) AS DECIMAL(12,6)) INTO l_hlcsHKD FROM SYSTEM.dual;--查询港币汇率
    SELECT CAST(F_GET_HLCS('3',i_rq) AS DECIMAL(12,6)) INTO l_hlcsUSD FROM SYSTEM.dual;--查询美元汇率
    
    l_suffix :='';
    l_sqlWhere :=' where rq='||i_rq;
    l_sqlWhereCjrq :=' where cjrq='||i_rq;
    
    --证券行情
    BEGIN
        l_tableName_sparkZqhqHisToday := F_IDS_GET_TABLENAME('sparkZqhqHisToday', NULL);
        l_sqlBuf:="select jys, zqdm, zqmc, jydw, zxj, zsp, jkp, zgj, zdj, cjsl, cjje, zxlx, lxjg, jjjybz, zxj_org, zsp_org, gzj_flag, rq  from  DSC_BAS.T_ZQHQ_XZ_HIS  where rq="||i_rq;
                    
        F_IDS_CREATE_TEMP_TABLE(l_sqlBuf,l_tableName_sparkZqhqHisToday);
    END;
    
    --证券类别
    BEGIN
        l_tableName_sparkZqlb := F_IDS_GET_TABLENAME('sparkZqlb', NULL);
        l_sqlBuf:="select * from dsc_cfg.VW_T_ZQLB_IDS";
                    
        F_IDS_CREATE_TEMP_TABLE(l_sqlBuf,l_tableName_sparkZqlb);
    END;    

    
    BEGIN
        l_tableName_sparkFzxxbdmx := F_IDS_GET_TABLENAME('xy_sparkJgmxlsXYQS', NULL);
        l_sqlBuf := 'select * from cust.t_xy_fzxxbdmx_his '||l_sqlWhere;
        
        F_IDS_CREATE_TEMP_TABLE(l_sqlBuf,l_tableName_sparkFzxxbdmx);
    END;
    
    BEGIN
        l_tableName_sparkFzxx := F_IDS_GET_TABLENAME('xy_sparkFzxx', NULL);
        l_sqlBuf := 'select * from CUST.T_XY_FZXX_HIS '||l_sqlWhere;
        
        F_IDS_CREATE_TEMP_TABLE(l_sqlBuf,l_tableName_sparkFzxx);
    END;
    

    
    BEGIN

        l_sqlBuf := "select 0 tzzh_id,rq,khh,jys,gdh,zqdm,zqlb,bz,zqsl,null as kcrq,null as fltsl,zxsz,
                 cccb as cccb,null as cbj,ljyk as ljyk,null as tbcccb,null as tbcbj,null as dryk, ZQSL AS ZQSL_ORIG
                 from dsc_bas.t_xy_zqye_his t where t.zqlb <> 'F8' and t.zqsl>0 AND T.ZQDM NOT LIKE 'SHRQ%' AND T.ZQDM NOT LIKE 'SZRQ%'
                 and zqdm not in  ('888880','900000')  and rq="||i_rq;       
        F_IDS_CREATE_TEMP_TABLE(l_sqlBuf,F_IDS_GET_TABLENAME('xy_sparkZqyeGt', NULL));
    END;
    
    BEGIN

        l_sqlBuf := "select * from DSC_STAT.T_STAT_KHZCXZMX where "||i_rq||" between ksrq and jsrq and zhlb in ('2')";       
        F_IDS_CREATE_TEMP_TABLE(l_sqlBuf,F_IDS_GET_TABLENAME('xy_sparkZcxz', NULL));--//资产修正在途
    END;
    
    
    BEGIN
        l_tableName_sparkZqyeGT:=F_IDS_GET_TABLENAME('xy_sparkZqyeGT_0', NULL);
        l_sqlBuf :="select 0 as TZZH_ID,
             KHH,
             JYS,
             GDH,
             ZQDM,  " 
             ||i_rq|| " as kcrq,
             ZQLB,
             BZ,
             ZQSL,
             FLTSL,
             ZXSZ, 
             cccb,
             ljyk,
             " ||i_rq|| " as rq
         FROM  "||F_IDS_GET_TABLENAME('xy_sparkZqyeGt', NULL);            
         
         F_IDS_CREATE_TEMP_TABLE(l_sqlBuf,l_tableName_sparkZqyeGT);
        END;
        BEGIN
            l_tableName_sparkZqyeGTResult:=F_IDS_GET_TABLENAME('xy_sparkZqyeGTResult', NULL);
            l_sqlBuf := "select
                   0 as tzzh_id,
                   khh,
                   jys,
                   gdh,
                   zqdm,
                   max(kcrq) as kcrq,
                   min(zqlb) as zqlb,
                   max(bz) as bz,
                   sum(zqsl) as zqsl,
                   sum(fltsl) as fltsl,
                   sum(zxsz) as zxsz,
                   sum(cccb) as cccb,
                   round(sum(cccb)/sum(zqsl), 4) as cbj,
                   sum(ljyk) as ljyk,
                   sum(cccb - ljyk) as tbcccb,
                   round(sum(cccb - ljyk)/sum(zqsl), 4) as tbcbj,
                   0 as dryk,
                   sum(zqsl) as zqsl_orig,
                   rq
                 from "||l_tableName_sparkZqyeGT||" group by khh,jys,gdh,zqdm,rq";
            
            F_IDS_CREATE_TEMP_TABLE(l_sqlBuf,l_tableName_sparkZqyeGTResult);
            F_IDS_OVERWRITE_PARTITION(l_tableName_sparkZqyeGTResult,"cust","t_xy_zqye_his",i_rq,NULL);
        END;
        
        --成本
        BEGIN
            l_tableName_sparkZqyeCbjsGT :=F_IDS_GET_TABLENAME('xy_sparkZqyeCbjsGT', NULL);
            l_sqlBuf:="SELECT nvl(t.TZZH_ID,0) as tzzh_id, KHH, JYS, GDH, ZQDM, KCRQ, ZQLB, BZ, ZQSL,ZXSZ AS ZQSZ,IF(CCCB<>0,CCCB,ZXSZ) AS CCCB,T.ljyk AS LJYK,
            ZQSL AS DRMRSL,ZQSL AS LJMRSL,ZQSL AS DRYSSL,ZXSZ AS DRMRJE,ZXSZ AS LJMRJE, RQ
              FROM CUST.T_XY_ZQYE_HIS T
             WHERE RQ = " ||i_rq;
             
             F_IDS_CREATE_TEMP_TABLE(l_sqlBuf,l_tableName_sparkZqyeCbjsGT);
             F_IDS_OVERWRITE_PARTITION(l_tableName_sparkZqyeCbjsGT,"cust","t_xy_zqye_cbjs",i_rq,NULL);
        END;
        
        --初始化
        BEGIN

            
            --1、生成资金余额数据
            BEGIN
                l_tableName_sparkzcZjye :=F_IDS_GET_TABLENAME('xy_sparkzcZjye', NULL);
                l_sqlBuf:="SELECT  tzzh_id,
                    KHH,
                    0 AS zzc_sr       ,
                    0 AS zzc         ,
                    0 AS zqsz         ,
                    CAST(ROUND(SUM(CASE
                            WHEN BZ = '2' THEN
                                ZHYE * " ||l_hlcsHKD||
                            "                WHEN BZ = '3' THEN
                                ZHYE * " ||l_hlcsUSD||
                            "                ELSE
                                ZHYE
                        END),2) AS DECIMAL(16,2)) AS ZJYE,
                    0 AS ztzc         ,
                    0 AS qtzc         ,
                    0 AS crje         ,
                    0 AS qcje         ,
                    0 AS zrsz         ,
                    0 AS zcsz         ,
                    0 AS zfz         ,
                    0 AS zfz_sr       ,
                    0 AS cccb         ,
                    0 AS dryk         ,
                    0 AS tzzh_fe     ,
                    0 AS tzzh_fe_sr   ,
                    0 AS tzzh_zxfe   ,
                    0 AS tzzh_zxfe_sr ,
                    0 AS tzzh_zxjz   ,
                    0 AS tzzh_zxjz_sr ,
                    0 AS tzzh_ljjz   ,
                    0 AS tzzh_ljjz_sr ,
                    0 AS rzfz         ,
                    0 AS rqfz         ,
                    0 AS yjlx         ,
                    0 AS ghlx         ,
                    0 AS yjlx_sr     ,
                    0 AS lxsr         ,
                    0 AS lxzc_qt     ,
                    0 AS qtsr        ,
                    0 AS qtzc_fy     ,
                    0 AS jyl         ,
                    0 AS xzrqfz       ,
                    0 AS xzrqyjlx     ,
                    0 AS xzrzyjlx     ,
                    0 AS xzfz         ,
                    0 AS xzhkje       ,
                    0 AS xzrqhkje     ,
                    0 AS xzrzhkje     ,
                    0 AS mqhqje       ,
                    0 AS xzghlx       ,
                    0 AS xzrzghlx     ,
                    0 AS xzrqghlx     ,
                    0 AS hkje         ,
                    0 AS mqhkje
                    FROM cust.t_xy_zjye_his T where rq=" ||i_rq||
                            " GROUP BY TZZH_ID,KHH";
                            
                F_IDS_CREATE_TEMP_TABLE(l_sqlBuf,l_tableName_sparkzcZjye);
            END;
            
            --2、生成证券市值数据
            BEGIN
                l_tableName_sparkzcZqsz :=F_IDS_GET_TABLENAME('xy_sparkzcZqsz', NULL);
                l_sqlBuf:="SELECT  tzzh_id,
                    KHH,
                    0 AS zzc_sr       ,
                    0 AS zzc         ,
                    CAST(ROUND(SUM(CASE
                            WHEN BZ = '2' THEN
                                ZXSZ * " ||l_hlcsHKD||
                            "                WHEN BZ = '3' THEN
                                ZXSZ * " ||l_hlcsUSD||
                            "                ELSE
                                ZXSZ
                        END),2) AS DECIMAL(16,2)) AS ZQSZ,
                    0 AS zjye         ,
                    0 AS ztzc         ,
                    0 AS qtzc         ,
                    0 AS crje         ,
                    0 AS qcje         ,
                    0 AS zrsz         ,
                    0 AS zcsz         ,
                    0 AS zfz         ,
                    0 AS zfz_sr       ,
                    CAST(ROUND(SUM(CASE
                            WHEN BZ = '2' THEN
                                nvl(CCCB,0) * " ||l_hlcsHKD||
                            "                WHEN BZ = '3' THEN
                                nvl(CCCB,0) * " ||l_hlcsUSD||
                            "                ELSE
                                nvl(CCCB,0)
                        END),2) AS DECIMAL(16,2)) AS CCCB,
                    0 AS dryk         ,
                    0 AS tzzh_fe     ,
                    0 AS tzzh_fe_sr   ,
                    0 AS tzzh_zxfe   ,
                    0 AS tzzh_zxfe_sr ,
                    0 AS tzzh_zxjz   ,
                    0 AS tzzh_zxjz_sr ,
                    0 AS tzzh_ljjz   ,
                    0 AS tzzh_ljjz_sr ,
                    0 AS rzfz         ,
                    0 AS rqfz         ,
                    0 AS yjlx         ,
                    0 AS ghlx         ,
                    0 AS yjlx_sr     ,
                    0 AS lxsr         ,
                    0 AS lxzc_qt     ,
                    0 AS qtsr        ,
                    0 AS qtzc_fy     ,
                    0 AS jyl         ,
                    0 AS xzrqfz       ,
                    0 AS xzrqyjlx     ,
                    0 AS xzrzyjlx     ,
                    0 AS xzfz         ,
                    0 AS xzhkje       ,
                    0 AS xzrqhkje     ,
                    0 AS xzrzhkje     ,
                    0 AS mqhqje       ,
                    0 AS xzghlx       ,
                    0 AS xzrzghlx     ,
                    0 AS xzrqghlx     ,
                    0 AS hkje         ,
                    0 AS mqhkje
              FROM cust.t_xy_zqye_his T /*LEFT SEMI JOIN "||l_tableName_sparkZqlb||" D ON (D.IS_JSSZ = 1 AND T.ZQLB =D.ZQLB)*/ where rq=" ||i_rq||
                            " GROUP BY TZZH_ID, KHH";
                
                F_IDS_CREATE_TEMP_TABLE(l_sqlBuf,l_tableName_sparkzcZqsz);
            END;
            
            --3、融资融券负债数据            
            BEGIN
                l_tableName_sparkzcFzxx :=F_IDS_GET_TABLENAME('xy_sparkzcFzxx', NULL);
                l_sqlBuf:="SELECT  TZZH_ID,
                    KHH,
                    0 AS zzc_sr       ,
                    0 AS zzc         ,
                    0 AS zqsz         ,
                    0 AS zjye         ,
                    0 AS ztzc         ,
                    0 AS qtzc         ,
                    0 AS crje         ,
                    0 AS qcje         ,
                    0 AS zrsz         ,
                    0 AS zcsz         ,
                    CAST(ROUND(SUM((RZFZ+RQFZ+YJLX+FXYJLX - GHLX) * CASE BZ WHEN '2' THEN " || l_hlcsHKD || " WHEN '3' THEN " || l_hlcsUSD || " ELSE 1 END),2) AS DECIMAL(16,2)) AS ZFZ,
                    0 AS zfz_sr       ,
                    0 AS cccb         ,
                    0 AS dryk         ,
                    0 AS tzzh_fe     ,
                    0 AS tzzh_fe_sr   ,
                    0 AS tzzh_zxfe   ,
                    0 AS tzzh_zxfe_sr ,
                    0 AS tzzh_zxjz   ,
                    0 AS tzzh_zxjz_sr ,
                    0 AS tzzh_ljjz   ,
                    0 AS tzzh_ljjz_sr ,
                    CAST(ROUND(SUM((RZFZ) * CASE BZ WHEN '2' THEN " || l_hlcsHKD || " WHEN '3' THEN " || l_hlcsUSD || " ELSE 1 END),2) AS DECIMAL(16,2)) AS RZFZ,
                    CAST(ROUND(SUM((RQFZ) * CASE BZ WHEN '2' THEN " || l_hlcsHKD || " WHEN '3' THEN " || l_hlcsUSD || " ELSE 1 END),2) AS DECIMAL(16,2)) AS RQFZ,
                    CAST(ROUND(SUM((YJLX+FXYJLX) * CASE BZ WHEN '2' THEN " || l_hlcsHKD || " WHEN '3' THEN " || l_hlcsUSD || " ELSE 1 END),2) AS DECIMAL(16,2)) AS YJLX,
                    CAST(ROUND(SUM((GHLX) * CASE BZ WHEN '2' THEN " || l_hlcsHKD || " WHEN '3' THEN " || l_hlcsUSD || " ELSE 1 END),2) AS DECIMAL(16,2)) AS GHLX,
                    0 AS yjlx_sr     ,
                    0 AS lxsr         ,
                    0 AS lxzc_qt     ,
                    0 AS qtsr        ,
                    0 AS qtzc_fy     ,
                    0 AS jyl         ,
                    0 AS xzrqfz       ,
                    0 AS xzrqyjlx     ,
                    0 AS xzrzyjlx     ,
                    0 AS xzfz         ,
                    0 AS xzhkje       ,
                    0 AS xzrqhkje     ,
                    0 AS xzrzhkje     ,
                    0 AS mqhqje       ,
                    0 AS xzghlx       ,
                    0 AS xzrzghlx     ,
                    0 AS xzrqghlx     ,
                CAST(ROUND(SUM(HKJE),2) AS DECIMAL(16,2)) AS HKJE ,
                    0 AS mqhkje
             FROM(SELECT 0 AS TZZH_ID,
                        BZ,
                        CASE
                            WHEN FZZT <> 3 AND JYLB = '61' THEN
                                nvl(FZBJ, 0) - nvl(HKJE, 0)
                            ELSE
                                0
                        END AS RZFZ, 
                        CASE
                            WHEN FZZT <> 3 AND JYLB = '64' THEN
                                nvl(ZXSZ, 0) + nvl(RQFY, 0) - nvl(HKJE, 0)
                            ELSE
                                0
                        END AS RQFZ,                    
                        JYLB,
                        CASE WHEN FZZT=0 THEN YJLX+nvl(FDLX,0) ELSE 0 END AS YJLX,
                        CASE WHEN FZZT <> 3 THEN GHLX ELSE 0 END AS GHLX,
                        HKJE,
                        KHH,
                        FXYJLX
                FROM "||l_tableName_sparkFzxx||" ) T GROUP BY TZZH_ID, KHH";
                F_IDS_CREATE_TEMP_TABLE(l_sqlBuf,l_tableName_sparkzcFzxx);
            END;
            
            --4、处理在途资产等修正数据
            BEGIN
                l_tableName_sparkzcZcxz :=F_IDS_GET_TABLENAME('xy_sparkzcZcxz', NULL);
                l_sqlBuf:= "SELECT 0 AS TZZH_ID,
                       KHH,
                        0 AS zzc_sr,
                        0 AS zzc,
                        0 AS zqsz,
                        0 AS zjye,
                       CAST(SUM(CASE
                             WHEN XZLB IN ('11', '12', '13', '14', '15', '22', '23', '6') THEN
                              XZJE * CASE BZ
                                WHEN '2' THEN
                                 " || l_hlcsHKD ||
                                "                WHEN '3' THEN
                                 " || l_hlcsUSD ||
                                "                ELSE
                                 1
                              END
                             ELSE
                              0
                           END) AS DECIMAL(16,2)) AS ZTZC,
                       CAST(SUM(CASE
                             WHEN XZLB IN ('1', '3', '4', '31', '32', '33', '34') THEN
                              XZJE * CASE BZ
                                WHEN '2' THEN
                                 " || l_hlcsHKD ||
                                "                WHEN '3' THEN
                                 " || l_hlcsUSD ||
                                "                ELSE
                                 1
                              END
                             ELSE
                              0
                           END) AS DECIMAL(16,2)) AS QTZC,
                        0 AS crje,
                        0 AS qcje,
                        0 AS zrsz,
                        0 AS zcsz,
                       CAST(SUM(CASE
                             WHEN XZLB IN ('2', '5') THEN
                              XZJE * CASE BZ
                                WHEN '2' THEN
                                 " || l_hlcsHKD ||
                                "                WHEN '3' THEN
                                 " || l_hlcsUSD ||
                                "                ELSE
                                 1
                              END
                             ELSE
                              0
                           END) AS DECIMAL(16,2)) AS ZFZ,
                        0 AS zfz_sr,
                        0 AS cccb,
                        0 AS dryk,
                        0 AS tzzh_fe,
                        0 AS tzzh_fe_sr,
                        0 AS tzzh_zxfe,
                        0 AS tzzh_zxfe_sr,
                        0 AS tzzh_zxjz,
                        0 AS tzzh_zxjz_sr,
                        0 AS tzzh_ljjz,
                        0 AS tzzh_ljjz_sr,
                        0 AS zqsz_jrcp,
                        0 AS lxsr,
                        0 AS zczj_totc,
                        0 AS zrzj_fotc,
                        0 AS lxzc_gpzy,
                        0 AS jyfy_gpzy,
                        0 AS lxzc_qt,
                        0 AS qtsr        ,
                    0 AS qtzc_fy     ,
                        0 AS jyl
                  FROM "|| F_IDS_GET_TABLENAME('xy_sparkZcxz', NULL) ||" T
                 GROUP BY TZZH_ID, KHH";
                F_IDS_CREATE_TEMP_TABLE(l_sqlBuf,l_tableName_sparkzcZcxz);
            END;
            
            BEGIN
                l_tableName_sparkzcFZxxbdmx :=F_IDS_GET_TABLENAME('xy_sparkzcFZxxbdmx', NULL);
                l_sqlBuf:= "SELECT  0 AS TZZH_ID,
                    D.KHH,
                    0 AS zzc_sr       ,
                    0 AS zzc         ,
                    0 AS zqsz         ,
                    0 AS zjye         ,
                    0 AS ztzc         ,
                    0 AS qtzc         ,
                    0 AS crje         ,
                    0 AS qcje         ,
                    0 AS zrsz         ,
                    0 AS zcsz         ,
                    0 AS zfz         ,
                    0 AS zfz_sr       ,
                    0 AS cccb         ,
                    0 AS dryk         ,
                    0 AS tzzh_fe     ,
                    0 AS tzzh_fe_sr   ,
                    0 AS tzzh_zxfe   ,
                    0 AS tzzh_zxfe_sr ,
                    0 AS tzzh_zxjz   ,
                    0 AS tzzh_zxjz_sr ,
                    0 AS tzzh_ljjz   ,
                    0 AS tzzh_ljjz_sr ,
                    0 AS rzfz         ,
                    0 AS rqfz         ,
                    0 AS yjlx         ,
                    0 AS ghlx         ,
                    0 AS yjlx_sr     ,
                    0 AS lxsr         ,
                    0 AS lxzc_qt     ,
                    0 AS qtsr        ,
                    0 AS qtzc_fy     ,
                    0 AS jyl         ,
                    CAST(ROUND(SUM(D.XZRQFZ),2) AS DECIMAL(16,2)) AS XZRQFZ,
                    CAST(ROUND(SUM(D.XZRQYJLX),2) AS DECIMAL(16,2)) AS XZRQYJLX,
                    CAST(ROUND(SUM(D.XZRZYJLX),2) AS DECIMAL(16,2)) AS XZRZYJLX,
                    CAST(ROUND(SUM(D.XZRQFZ) + SUM(D.XZRQYJLX) + SUM(D.XZRZYJLX),2) AS DECIMAL(16,2)) AS XZFZ,
                    CAST(ROUND(SUM(D.XZHKJE),2) AS DECIMAL(16,2)) AS XZHKJE,
                    CAST(ROUND(SUM(D.XZRQHKJE),2) AS DECIMAL(16,2)) AS XZRQHKJE,
                    CAST(ROUND(SUM(D.XZRZHKJE),2) AS DECIMAL(16,2)) AS XZRZHKJE,
                    0 AS mqhqje       ,
                    CAST(ROUND(SUM(D.XZGHLX),2) AS DECIMAL(16,2)) AS XZGHLX,
                    CAST(ROUND(SUM(D.XZRZGHLX),2) AS DECIMAL(16,2)) AS XZRZGHLX,
                    CAST(ROUND(SUM(D.XZRQGHLX),2) AS DECIMAL(16,2)) AS XZRQGHLX,
                    0 AS hkje         ,
                    0 AS mqhkje
                 FROM "||l_tableName_sparkFzxxbdmx||" D
                 GROUP BY D.KHH";
             
                F_IDS_CREATE_TEMP_TABLE(l_sqlBuf,l_tableName_sparkzcFZxxbdmx);
            END;
            
            
            
            BEGIN
                l_tableName_sparkStatDR :=F_IDS_GET_TABLENAME('xy_sparkStatDR', NULL);
                l_sqlBuf:="select * from "||l_tableName_sparkzcZjye||"
                            UNION ALL
                            select * from "||l_tableName_sparkzcZqsz||"
                            UNION ALL
                            select * from "||l_tableName_sparkzcFzxx||"
                            UNION ALL
                            select * from "||l_tableName_sparkzcFZxxbdmx||"
                            UNION ALL
                            select 0 AS TZZH_ID,KHH,zzc_sr,zzc,zqsz,zjye,ztzc,qtzc,crje,qcje,zrsz,zcsz,ZFZ,zfz_sr,cccb,dryk,tzzh_fe,tzzh_fe_sr,tzzh_zxfe,tzzh_zxfe_sr,tzzh_zxjz,tzzh_zxjz_sr,tzzh_ljjz,tzzh_ljjz_sr,0 as RZFZ,0 as RQFZ,0 as YJLX,0 as GHLX,0 as yjlx_sr,lxsr,lxzc_qt,0 AS qtsr,
                    0 AS qtzc_fy ,jyl,0 as xzrqfz,0 as xzrqyjlx,0 as xzrzyjlx,0 as xzfz,0 as xzhkje,0 as xzrqhkje,0 as xzrzhkje,0 as mqhqje,0 as xzghlx,0 as xzrzghlx,0 as xzrqghlx,0 as HKJE,0 as mqhkje  from "||l_tableName_sparkzcZcxz;
                            
                F_IDS_CREATE_TEMP_TABLE(l_sqlBuf,l_tableName_sparkStatDR);
            END;
            
            BEGIN
                l_tableName_sparkZctjResult :=F_IDS_GET_TABLENAME('xy_sparkZctjResult', NULL);
                l_sqlBuf:="SELECT  TZZH_ID,
                    KHH,
                    CAST(ROUND((CASE WHEN ZZC_SR = 0 AND ZZC > 0 THEN ZZC - DRYK ELSE ZZC_SR END),2) AS DECIMAL(16,2)) AS ZZC_SR,
                    CAST(ROUND(ZZC,2) AS DECIMAL(16,2)) AS ZZC,
                    CAST(ROUND(ZQSZ,2) AS DECIMAL(16,2)) AS ZQSZ,
                    CAST(ROUND(ZJYE,2) AS DECIMAL(16,2)) AS ZJYE,
                    CAST(ROUND(ZTZC,2) AS DECIMAL(16,2)) AS ZTZC,
                    CAST(ROUND(QTZC,2) AS DECIMAL(16,2)) AS QTZC,
                    CAST(ROUND(CRJE,2) AS DECIMAL(16,2)) AS CRJE,
                    CAST(ROUND(QCJE,2) AS DECIMAL(16,2)) AS QCJE,
                    CAST(ROUND(ZRSZ,2) AS DECIMAL(16,2)) AS ZRSZ,
                    CAST(ROUND(ZCSZ,2) AS DECIMAL(16,2)) AS ZCSZ,
                    CAST(ROUND(ZFZ,2) AS DECIMAL(16,2)) AS ZFZ,
                    CAST(ROUND(ZFZ_SR,2) AS DECIMAL(16,2)) AS ZFZ_SR,
                    CAST(ROUND(CCCB,2) AS DECIMAL(16,2)) AS CCCB,
                    CAST(ROUND(DRYK,2) AS DECIMAL(16,2)) AS DRYK,
                    CAST(ROUND((CASE WHEN ZZC_SR = 0 AND ZZC > 0 THEN ZZC - ZFZ - DRYK ELSE TZZH_FE END),2) AS DECIMAL(16,2)) AS TZZH_FE,
                    CAST(ROUND((CASE WHEN ZZC_SR = 0 AND ZZC > 0 THEN ZZC - ZFZ - DRYK ELSE TZZH_FE_SR END),2) AS DECIMAL(16,2)) AS TZZH_FE_SR,
                    CAST(ROUND((CASE WHEN ZZC_SR = 0 AND ZZC > 0 THEN ZZC - ZFZ - DRYK ELSE TZZH_ZXFE END),2) AS DECIMAL(16,2)) AS TZZH_ZXFE,
                    CAST(ROUND((CASE WHEN ZZC_SR = 0 AND ZZC > 0 THEN ZZC - ZFZ - DRYK ELSE TZZH_ZXFE_SR END),2) AS DECIMAL(16,2)) AS TZZH_ZXFE_SR,
                    CAST(ROUND((CASE WHEN ZZC_SR = 0 AND ZZC > 0 THEN 1.0 ELSE 1.0 END),4) AS DECIMAL(10,4)) AS TZZH_ZXJZ,
                    CAST(ROUND((CASE WHEN ZZC_SR = 0 AND ZZC > 0 THEN 1.0 ELSE 1.0 END),4) AS DECIMAL(10,4)) AS TZZH_ZXJZ_SR,
                    CAST(ROUND((CASE WHEN ZZC_SR = 0 AND ZZC > 0 THEN 1.0 ELSE 1.0 END),4) AS DECIMAL(22,4)) AS TZZH_LJJZ,
                    CAST(ROUND((CASE WHEN ZZC_SR = 0 AND ZZC > 0 THEN 1.0 ELSE 1.0 END),4) AS DECIMAL(22,4)) AS TZZH_LJJZ_SR,
                    CAST(ROUND(RZFZ,2) AS DECIMAL(16,2)) AS RZFZ,
                    CAST(ROUND(RQFZ,2) AS DECIMAL(16,2)) AS RQFZ,
                    CAST(ROUND(YJLX,2) AS DECIMAL(16,2)) AS YJLX,
                    CAST(ROUND(GHLX,2) AS DECIMAL(16,2)) AS GHLX,
                    CAST(0 AS DECIMAL(16,2)) AS YJLX_SR,
                    CAST(ROUND(LXSR,2) AS DECIMAL(16,2)) AS LXSR,
                    CAST(ROUND(LXZC_QT,2) AS DECIMAL(16,2)) AS LXZC_QT,
                    CAST(ROUND(QTSR,2) AS DECIMAL(16,2)) AS QTSR,
                    CAST(ROUND(QTZC_FY,2) AS DECIMAL(16,2)) AS QTZC_FY,
                    CAST(ROUND(JYL,2) AS DECIMAL(16,2)) AS JYL,
                    CAST(ROUND(XZRQFZ,2) AS DECIMAL(16,2)) AS XZRQFZ,
                    CAST(ROUND(XZRQYJLX,2) AS DECIMAL(16,2)) AS XZRQYJLX,
                    CAST(ROUND(XZRZYJLX,2) AS DECIMAL(16,2)) AS XZRZYJLX,
                    CAST(ROUND(XZFZ,2) AS DECIMAL(16,2)) AS XZFZ,
                    CAST(ROUND(XZHKJE,2) AS DECIMAL(16,2)) AS XZHKJE,
                    CAST(ROUND(XZRQHKJE,2) AS DECIMAL(16,2)) AS XZRQHKJE,
                    CAST(ROUND(XZRZHKJE,2) AS DECIMAL(16,2)) AS XZRZHKJE,
                    CAST(ROUND(MQHQJE,2) AS DECIMAL(16,2)) AS MQHQJE,
                    CAST(ROUND(XZGHLX,2) AS DECIMAL(16,2)) AS XZGHLX,
                    CAST(ROUND(XZRQGHLX,2) AS DECIMAL(16,2)) AS XZRQGHLX,
                    CAST(ROUND(XZRZGHLX,2) AS DECIMAL(16,2)) AS XZRZGHLX,
                    CAST(ROUND(HKJE,2) AS DECIMAL(16,2)) AS HKJE,
                    CAST(ROUND(MQHKJE,2) AS DECIMAL(16,2)) AS MQHKJE,  " ||i_rq||" as RQ
                 FROM
                 (SELECT  nvl(TZZH_ID, 0) AS TZZH_ID,
                    t.KHH,
                    nvl(ZZC_SR, 0) AS ZZC_SR,
                    nvl(ZZC, 0) AS ZZC,
                    nvl(ZQSZ, 0) AS ZQSZ,
                    nvl(ZJYE, 0) AS ZJYE,
                    nvl(ZTZC, 0) AS ZTZC,
                    nvl(QTZC, 0) AS QTZC,
                    nvl(CRJE, 0) AS CRJE,
                    nvl(QCJE, 0) AS QCJE,
                    nvl(ZRSZ, 0) AS ZRSZ,
                    nvl(ZCSZ, 0) AS ZCSZ,
                    nvl(ZFZ_SR, 0) AS ZFZ_SR,
                    nvl(ZFZ, 0) AS ZFZ,
                    nvl(CCCB, 0) AS CCCB,
                    0 AS DRYK,
                    ROUND(nvl(TZZH_FE,0),2) AS TZZH_FE,
                    nvl(TZZH_FE_SR, 0) AS TZZH_FE_SR,
                    nvl(TZZH_ZXFE,0) AS TZZH_ZXFE,
                    nvl(TZZH_ZXFE_SR, 0) AS TZZH_ZXFE_SR,
                    IF(nvl(TZZH_ZXFE,0) = 0, 1.0, ROUND((nvl(ZZC,0)-nvl(ZFZ,0)) / TZZH_ZXFE,4)) AS TZZH_ZXJZ,
                    nvl(TZZH_ZXJZ_SR, 0) AS TZZH_ZXJZ_SR,
                    IF(nvl(TZZH_FE,0) = 0, 1.0, ROUND((nvl(ZZC,0)-nvl(ZFZ,0)) / TZZH_FE,4)) AS TZZH_LJJZ,
                    nvl(TZZH_LJJZ_SR, 0) AS TZZH_LJJZ_SR,
                    nvl(RZFZ, 0) AS RZFZ,
                    nvl(RQFZ, 0) AS RQFZ,
                    nvl(YJLX, 0) AS YJLX,
                    nvl(GHLX, 0) AS GHLX,
                    nvl(LXSR, 0) AS LXSR,
                    nvl(LXZC_QT, 0) AS LXZC_QT,
                    nvl(QTSR, 0) AS QTSR,
                    nvl(QTZC_FY, 0) AS QTZC_FY,
                    nvl(JYL, 0) AS JYL,
                    nvl(XZRQFZ, 0) AS XZRQFZ,
                    nvl(XZRQYJLX, 0) AS XZRQYJLX,
                    nvl(XZRZYJLX, 0) AS XZRZYJLX,
                    nvl(XZFZ, 0) AS XZFZ,
                    nvl(XZHKJE, 0) AS XZHKJE,
                    nvl(XZRQHKJE, 0) AS XZRQHKJE,
                    nvl(XZRZHKJE, 0) AS XZRZHKJE,
                    nvl(MQHQJE, 0) AS MQHQJE,
                    nvl(XZGHLX, 0) AS XZGHLX,
                    nvl(XZRQGHLX, 0) AS XZRQGHLX,
                    nvl(XZRZGHLX, 0) AS XZRZGHLX,
                    nvl(HKJE, 0) AS HKJE,
                    nvl(MQHKJE, 0) AS MQHKJE
                 FROM
                  (SELECT TZZH_ID,
                        s.KHH,
                        nvl(SUM(ZZC_SR), 0) AS ZZC_SR,
                        nvl(SUM(ZQSZ), 0) + nvl(SUM(ZJYE), 0) + nvl(SUM(ZTZC), 0) +
                        nvl(SUM(QTZC), 0) AS ZZC,
                        nvl(SUM(ZQSZ), 0) AS ZQSZ,
                        nvl(SUM(ZJYE), 0) AS ZJYE,
                        nvl(SUM(ZTZC), 0) AS ZTZC,
                        nvl(SUM(QTZC), 0) AS QTZC,
                        nvl(SUM(CRJE), 0) AS CRJE,
                        nvl(SUM(QCJE), 0) AS QCJE,
                        nvl(SUM(ZRSZ), 0) AS ZRSZ,
                        nvl(SUM(ZCSZ), 0) AS ZCSZ,
                        nvl(SUM(ZFZ_SR), 0) AS ZFZ_SR,
                        nvl(SUM(ZFZ), 0) AS ZFZ,
                        nvl(SUM(RZFZ), 0) AS RZFZ,
                        nvl(SUM(RQFZ), 0) AS RQFZ,
                        nvl(SUM(YJLX), 0) AS YJLX,
                        nvl(SUM(YJLX_SR), 0) AS YJLX_SR,
                        nvl(SUM(GHLX), 0) AS GHLX,
                        nvl(SUM(CCCB), 0) AS CCCB,
                        CASE
                                WHEN nvl(SUM(TZZH_LJJZ_SR), 1.0)=0 THEN
                          nvl(SUM(ZQSZ), 0) + nvl(SUM(ZJYE), 0) +
                                nvl(SUM(ZTZC), 0) + nvl(SUM(QTZC), 0) - nvl(SUM(ZFZ),0)
                        ELSE
                          IF(SUM(TZZH_ZXFE_SR) is NULL,
                                    nvl(SUM(ZQSZ), 0) + nvl(SUM(ZJYE), 0) +
                                    nvl(SUM(ZTZC), 0) + nvl(SUM(QTZC), 0) - nvl(SUM(ZFZ),0) ,
                                    nvl(SUM(TZZH_FE_SR),0) +
                                    (nvl(SUM(CRJE),0) + nvl(SUM(LXSR),0) - nvl(SUM(QCJE),0) - nvl(SUM(LXZC_QT), 0) + nvl(SUM(QTSR), 0) - NVL(SUM(QTZC_FY), 0) + nvl(SUM(ZRSZ),0) - nvl(SUM(ZCSZ),0)) / nvl(SUM(TZZH_LJJZ_SR), 1.0))
                      END AS TZZH_FE,
                        nvl(SUM(TZZH_FE_SR), 0) AS TZZH_FE_SR,
                        nvl(SUM(TZZH_LJJZ_SR), 0) AS TZZH_LJJZ_SR,
                      CASE
                                WHEN nvl(SUM(TZZH_ZXJZ_SR), 1.0)=0 THEN
                          nvl(SUM(ZQSZ), 0) + nvl(SUM(ZJYE), 0) +
                                nvl(SUM(ZTZC), 0) + nvl(SUM(QTZC), 0)- nvl(SUM(ZFZ),0)
                        ELSE
                          IF(SUM(TZZH_ZXFE_SR) is NULL,
                                    nvl(SUM(ZQSZ), 0) + nvl(SUM(ZJYE), 0) +
                                    nvl(SUM(ZTZC), 0) + nvl(SUM(QTZC), 0) - nvl(SUM(ZFZ),0) ,
                                    nvl(SUM(TZZH_ZXFE_SR),0) +
                                    (nvl(SUM(CRJE),0) + nvl(SUM(LXSR),0) - nvl(SUM(QCJE),0)- nvl(SUM(LXZC_QT), 0) + nvl(SUM(QTSR), 0) - NVL(SUM(QTZC_FY), 0) + nvl(SUM(ZRSZ),0) - nvl(SUM(ZCSZ),0)) / nvl(SUM(TZZH_ZXJZ_SR), 1.0))
                      END AS TZZH_ZXFE,
                        nvl(SUM(TZZH_ZXFE_SR), 0) AS TZZH_ZXFE_SR,
                        nvl(SUM(TZZH_ZXJZ_SR), 0) AS TZZH_ZXJZ_SR,
                        nvl(SUM(LXSR), 0) AS LXSR,
                      nvl(SUM(LXZC_QT), 0) AS LXZC_QT,
                      nvl(SUM(QTSR), 0) AS QTSR,
                      nvl(SUM(QTZC_FY), 0) AS QTZC_FY,
                      nvl(SUM(JYL), 0) AS JYL,
                        nvl(SUM(XZRQFZ), 0) AS XZRQFZ,
                        nvl(SUM(XZRQYJLX), 0) AS XZRQYJLX,
                      nvl(SUM(XZRZYJLX), 0) AS XZRZYJLX,
                      nvl(SUM(XZFZ), 0) AS XZFZ,
                        nvl(SUM(XZHKJE), 0) AS XZHKJE,
                        nvl(SUM(XZRQHKJE), 0) AS XZRQHKJE,
                      nvl(SUM(XZRZHKJE), 0) AS XZRZHKJE,
                      nvl(SUM(MQHQJE), 0) AS MQHQJE,
                        nvl(SUM(XZGHLX), 0) AS XZGHLX,
                        nvl(SUM(XZRQGHLX), 0) AS XZRQGHLX,
                      nvl(SUM(XZRZGHLX), 0) AS XZRZGHLX,
                      nvl(SUM(HKJE), 0) AS HKJE,
                      nvl(SUM(MQHKJE), 0) AS MQHKJE
                   FROM "||l_tableName_sparkStatDR||" s
                  GROUP BY TZZH_ID, s.KHH) t left join cust.t_khxx_jjyw k on (t.khh=k.khh)
                  WHERE k.khrq <=  " || I_RQ || "
				        and (k.khzt != '3' or (k.khzt = '3' and k.xhrq >= " || I_RQ || " ))
                )T";
                                            
                F_IDS_CREATE_TEMP_TABLE(l_sqlBuf,l_tableName_sparkZctjResult);
			-- 净值折算
	BEGIN
	l_tableName := l_dbname||'xy_sparkZsjzResult'||l_suffix;	
	l_sqlBuf :="select  DR.TZZH_ID,
						DR.KHH,
						DR.ZZC_SR,
						DR.ZZC,
						DR.ZQSZ,
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
						DR.RZFZ,
						DR.RQFZ,
						DR.YJLX,
						DR.GHLX,
						DR.YJLX_SR,
						DR.LXSR,
						DR.LXZC_QT,
						DR.QTSR,
						DR.QTZC_FY,
						DR.JYL,
						DR.XZRQFZ,
						DR.XZRQYJLX,
						DR.XZRZYJLX,
						DR.XZFZ,
						DR.XZHKJE,
						DR.XZRQHKJE,
						DR.XZRZHKJE,
						DR.MQHQJE,
						DR.XZGHLX,
						DR.XZRQGHLX,
						DR.XZRZGHLX,
						DR.HKJE,
						DR.MQHKJE,
						-- 新增字段存储折算标志、cache、折算前份额和净值，便于数据核对
						zs.jzzzl,
						zs.reload,
						zs.cache,
						DR.TZZH_ZXFE AS TZZH_ZXFE_OLD,
						DR.TZZH_ZXJZ AS TZZH_ZXJZ_OLD,
						DR.TZZH_LJJZ AS TZZH_LJJZ_OLD,
						DR.rq
				from "|| l_dbname||'xy_sparkZctjResult'||l_suffix||" dr
				left join (SELECT   ConvertWorth(z.khh, 
									nvl(z.zzc,0.0), 
									nvl(z.zfz,0.0), 
									nvl(z.crje,0.0), 
									nvl(z.qcje,0.0), 
									nvl(z.zrsz,0.0), 
									nvl(z.zcsz,0.0), 
									nvl(z.dryk,0.0), 
									nvl(z.qtsr,0.0), 
									nvl(NULL,0.0), 
									nvl(NULL,0.0), 
									nvl(NULL, 1.0), 
									nvl(NULL,''))    -- 初始化无需获取上日数据，直接取null
						   FROM  "||l_dbname||'xy_sparkZctjResult'||l_suffix||" z ) zs 
					   on dr.khh = zs.khh ";
	F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
	
  END;
  
                F_IDS_OVERWRITE_PARTITION(l_tableName,"cust","t_stat_xy_zc_r",i_rq,NULL);
            END;
        END;
 END;
 /