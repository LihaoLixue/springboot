!set plsqlUseSlash true
CREATE OR REPLACE PROCEDURE cust.p_ids_daily_bill_njzq(
                            i_rq in int,
                            i_khh in STRING 
)is
/*********************************************************************************************
    *文件名称：CUST.P_IDS_DAILY_BILL
    *项目名称：IDS计算
    *文件说明：日全账

    创建人：朱强生
    功能说明：日全账
    
    修改者            版本号            修改日期            说明
    朱强生         v1.0.0            2019/6/27            创建
    燕居庆         v1.0.0            2019/07/10           处理语法错误，新增上日资产写入
    燕居庆         v1.0.1            2019/08/05           生成南京证券个性化指标
*********************************************************************************************/
DECLARE 
    l_sqlBuf STRING; --创建表语句
    l_tableName STRING; --临时表名
    l_sqlWhereCurrentDay STRING; 
    l_dbname STRING; --表前缀
    l_suffix STRING; --表后缀，为了单客户计算，添加khh后缀
    l_sqlWhereLastDay STRING;
    l_lastDay INT;
    l_sql STRING;
    l_khh STRING;
    l_hlcsHKD DECIMAL(12,6);
    l_hlcsUSD DECIMAL(12,6);
    TYPE nest_table IS TABLE OF STRING;
    l_tableArr nest_table DEFAULT NULL;
    l_columns STRING;
--变量    
    l_isMonthBegin BOOLEAN;
    l_isYearBegin BOOLEAN;
    l_hbaseName  STRING;
    l_hbaseTable STRING;
    l_hbaseDir STRING;
    l_dataDay INT;
    
    hs300Zxj DOUBLE;
    hs300Zzl DOUBLE;    
    
BEGIN
    BEGIN
    --1.1 获取汇率参数
    SELECT CAST(F_GET_HLCS('2',I_RQ) AS DECIMAL(12,6)) INTO l_hlcsHKD FROM `SYSTEM`.dual;
    SELECT CAST(F_GET_HLCS('3',I_RQ) AS DECIMAL(12,6)) INTO l_hlcsUSD FROM `SYSTEM`.dual;
  END;
  -- 获取上一交易日
  SELECT F_GET_JYR_DATE(I_RQ, -1) INTO l_lastDay FROM system.dual;
  
  IF substr(l_lastDay, 1, 6) = substr(I_RQ, 1, 6) THEN
    l_isMonthBegin := FALSE;
  ELSE
    l_isMonthBegin := TRUE;
  END IF;
  
  IF substr(l_lastDay, 1, 4) = substr(I_RQ, 1, 4) THEN
    l_isYearBegin := FALSE;
  ELSE
    l_isYearBegin := TRUE;
  END IF;
  
  IF I_KHH IS NULL THEN
    l_sqlWhereCurrentDay := I_RQ;
    l_sqlWhereLastDay := l_lastDay || ' ';
  ELSE 
    l_sqlWhereCurrentDay := I_RQ || ' and khh = ' || I_KHH;
    l_sqlWhereLastDay := l_lastDay || ' and khh = ' || I_KHH;
  END IF;
  -- 临时表创建temp库下
  l_dbname := "tempspark."; 
  IF I_KHH IS NULL THEN
    l_sqlWhereLastDay := l_lastDay;
    l_sqlWhereCurrentDay := I_RQ;
    l_suffix := '';
    l_khh:='';
  ELSE 
    l_sqlWhereLastDay := l_lastDay || ' and khh = ' || I_KHH;
    l_sqlWhereCurrentDay := I_RQ || ' and khh = ' || I_KHH;
    l_suffix := '_' || I_KHH;
    l_khh :=' and a.khh='||I_KHH;    
  END IF;
  
    BEGIN
  ----------------------------------------//日均统计-----------------------------------------------------
    /**
     * 临时表名
     * 需要添加相关前缀：
     */
    l_tableName := l_dbname || 'sparkSrzd' || l_suffix;
    l_sqlBuf := 'select * from CUST.t_stat_zd_r D where rq = '|| l_sqlWhereLastDay;
    
    /**
     * 调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
     */
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName)
  END;
  
  BEGIN
  ----------------------------------------//集中交易日资产统计-----------------------------------------------------
    /**
     * 临时表名
     * 需要添加相关前缀：
     */
    l_tableName := l_dbname || 'sparkJzjyZc' || l_suffix;
    l_sqlBuf := 'select * from CUST.t_stat_zc_r D where rq = '||l_sqlWhereCurrentDay;
    
    /**
     * 调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
     */
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName)
  END;
  
  BEGIN
  ----------------------------------------//信用日资产统计-----------------------------------------------------
    /**
     * 临时表名
     * 需要添加相关前缀：
     */
    l_tableName := l_dbname || 'sparkXyZc' || l_suffix;
    l_sqlBuf := 'select * from CUST.t_stat_xy_zc_r D where  rq= '||l_sqlWhereCurrentDay;
    
    /**
     * 调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
     */
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName)
  END;
  BEGIN
  ----------------------------------------//个股期权日资产统计-----------------------------------------------------
    /**
     * 临时表名
     * 需要添加相关前缀：
     */
    l_tableName := l_dbname || 'sparkSoZc' || l_suffix;
    l_sqlBuf := 'select * from CUST.t_stat_so_zc_r D where rq = '||l_sqlWhereCurrentDay
    
    /**
     * 调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
     */
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName)
  END;
  
   BEGIN
  ----------------------------------------//集中交易持仓-----------------------------------------------------
    /**
     * 临时表名
     * 需要添加相关前缀：
     */
    l_tableName := l_dbname || 'sparkJzjyZqye' || l_suffix;
    l_sqlBuf := 'select * from CUST.t_zqye_his D where rq = '||l_sqlWhereCurrentDay;
    
    /**
     * 调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
     */
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName)
  END;
   BEGIN
  ----------------------------------------//两融持仓-----------------------------------------------------
    /**
     * 临时表名
     * 需要添加相关前缀：
     */
    l_tableName := l_dbname || 'sparkXyZqye' || l_suffix;
    l_sqlBuf := 'select * from CUST.t_xy_zqye_his D where rq = '||l_sqlWhereCurrentDay;
    
    /**
     * 调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
     */
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName)
  END;
  
   BEGIN
  ----------------------------------------//金融产品份额（持仓）-----------------------------------------------------
    /**
     * 临时表名
     * 需要添加相关前缀：
     */
    l_tableName := l_dbname || 'sparkFpCpfe' || l_suffix;
    l_sqlBuf := 'select * from CUST.t_fp_cpfe_his D where rq = '||l_sqlWhereCurrentDay;
    
    /**
     * 调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
     */
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName)
  END;
  
   BEGIN
  ----------------------------------------//金融产品份额柜台-----------------------------------------------------
    /**
     * 临时表名
     * 需要添加相关前缀：
     */
    l_tableName := l_dbname || 'sparkFpCpfeGt' || l_suffix;
    l_sqlBuf := 'select * from dsc_bas.t_fp_cpfe_his D where rq = '||l_sqlWhereCurrentDay;
    
    /**
     * 调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
     */
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName)
  END;
  
    BEGIN
  ----------------------------------------//期权持仓-----------------------------------------------------
    /**
     * 临时表名
     * 需要添加相关前缀：
     */
    l_tableName := l_dbname || 'sparkSoHycc' || l_suffix;
    l_sqlBuf := 'select * from CUST.t_so_zqye_his D where rq = '||l_sqlWhereCurrentDay;
    
    /**
     * 调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
     */
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName)
  END;
  
   BEGIN
  ----------------------------------------//集中交易清仓-----------------------------------------------------
    /**
     * 临时表名
     * 需要添加相关前缀：
     */
    l_tableName := l_dbname || 'sparkJzjyTzsy' || l_suffix;
    l_sqlBuf := 'select * from CUST.t_tzsy D where qcrq = '||l_sqlWhereCurrentDay;
    
    /**
     * 调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
     */
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName)
  END;
  
  BEGIN
  ----------------------------------------//两融清仓-----------------------------------------------------
    /**
     * 临时表名
     * 需要添加相关前缀：
     */
    l_tableName := l_dbname || 'sparkXyTzsy' || l_suffix;
    l_sqlBuf := 'select * from CUST.t_xy_tzsy D where qcrq = '||l_sqlWhereCurrentDay;
    
    /**
     * 调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
     */
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName)
  END;
   BEGIN
  ----------------------------------------//金融产品清仓-----------------------------------------------------
    /**
     * 临时表名
     * 需要添加相关前缀：
     */
    l_tableName := l_dbname || 'sparkFpTzsy' || l_suffix;
    l_sqlBuf := 'select * from CUST.t_fp_tzsy D where qcrq = '||l_sqlWhereCurrentDay;
    
    /**
     * 调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
     */
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName)
  END;
  
   BEGIN
  ----------------------------------------//期权清仓-----------------------------------------------------
    /**
     * 临时表名
     * 需要添加相关前缀：
     */
    l_tableName := l_dbname || 'sparkSoTzsy' || l_suffix;
    l_sqlBuf := 'select * from CUST.t_so_tzsy D where qcrq = '||l_sqlWhereCurrentDay;
    
    /**
     * 调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
     */
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName)
  END;
  
     BEGIN
  ----------------------------------------//集中交易交割-----------------------------------------------------
    /**
     * 临时表名
     * 需要添加相关前缀：
     */
    l_tableName := l_dbname || 'sparkJzjyJgls' || l_suffix;
    l_sqlBuf := 'select * from CUST.t_jgmxls_his D where cjrq = '||l_sqlWhereCurrentDay;
    
    /**
     * 调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
     */
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName)
  END;
  
       BEGIN
  ----------------------------------------//两融交割-----------------------------------------------------
    /**
     * 临时表名
     * 需要添加相关前缀：
     */
    l_tableName := l_dbname || 'sparkXyJgls' || l_suffix;
    l_sqlBuf := 'select * from CUST.t_xy_jgmxls_his D where cjrq = '||l_sqlWhereCurrentDay;
    
    /**
     * 调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
     */
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName)
  END;
  
    BEGIN
    l_tableName := l_dbname || 'sparkParamValue' || l_suffix;
    l_sqlBuf := 'select * from dsc_cfg.t_param_value';
    
    /**
     * 调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
     */
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName)
  END;
  
  BEGIN
    l_tableName := l_dbname || 'sparkZqdmbg' || l_suffix;
    l_sqlBuf := 'select * from INFO.TZQDMBG';
    
    /**
     * 调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
     */
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName)
  END;
  
    --证券类别
    BEGIN
        l_tableName :=l_dbname||'sparkZqlb';
        l_sqlBuf:="select * from dsc_cfg.vw_t_zqlb_ids";
                    
        F_IDS_CREATE_TEMP_TABLE(l_sqlBuf,l_tableName);
    END;
    
    --证券品种
    begin
        l_tableName := F_IDS_GET_TABLENAME('sparkZqpzDy', I_KHH);
        l_sqlBuf := "SELECT
                    DISTINCT JYS
                              , ZQLB
                              , JB3_ZQPZ AS ZQPZ
                FROM
                    dsc_cfg.t_zqpz_dy
                WHERE
                     IS_VALID = 1
                     AND is_jssz = 1
                     AND jb3_zqpz <> 1102";
                     
        F_IDS_CREATE_TEMP_TABLE(l_sqlBuf,l_tableName);
    end;
    
   BEGIN
        l_tableName:=l_dbname||'sparkZcpz'|| l_suffix;
        l_sqlBuf:= " select zqdm,jys,zqlb,zcpzflbm,zcpzflmc FROM "||
                    " (select zqdm,jys,zqlb,zcpzflbm,zcpzflmc,row_number() over(partition by jys,zqdm order by whrq desc) rn"||
                    " from info.TZQDM_ZCPZWH) a where rn=1"        
        F_IDS_CREATE_TEMP_TABLE(l_sqlBuf,l_tableName);
    END;
  

    
    
    BEGIN
  ----------------------------------------//持仓及清仓-----------------------------------------------------
    /**
     * 临时表名
     * 需要添加相关前缀：
     */
    l_tableName := l_dbname || 'sparkTzfb' || l_suffix;
    l_sqlBuf := "select  khh,"||
                "        dryk,"||
                " case when z.lb='fp' then '理财' when z.lb='so' then '期权' else nvl(a.zqpzmc,'其他') end as zqpz,"||
                " case when z.lb in ('fp','so') then '权益类' else nvl(b.zcpzflmc, '其他') end as zcpz,"||
                "       z.lb,"||
                "       z.fdyk,"||
                "       z.zxsz from "||
                " (select khh,jys,zqdm, decode(bz, 2, cast(dryk as double) * "|| l_hlcsHKD ||", 3, cast(dryk as double) * "|| l_hlcsUSD ||", cast(dryk as double) ) as dryk,zqlb,'jzjy' as lb,decode(bz, 2, cast(zxsz-tbcccb as double) *" || l_hlcsHKD ||", 3, cast(zxsz-tbcccb as double) * " || l_hlcsUSD 
                ||", cast(zxsz-tbcccb as double)) as fdyk,decode(bz, 2, cast(zxsz as double) * "|| l_hlcsHKD ||", 3,  cast(zxsz as double) * "|| l_hlcsUSD ||", cast(zxsz as double)) as zxsz"||
                "  from "||  F_IDS_GET_TABLENAME('sparkJzjyZqye', I_KHH) ||" union ALL "|| 
                "  select khh,jys,zqdm,decode(bz, 2, cast(dryk as double) * "|| l_hlcsHKD ||", 3, cast(dryk as double) * "|| l_hlcsUSD ||", cast(dryk as double) ) as dryk,zqlb,'jzjy' as lb,0.0 as fdyk,0.0 as zxsz"||
                "  from "||  F_IDS_GET_TABLENAME('sparkJzjyTzsy', I_KHH)||"  union ALL"||
                " select khh,jrjgdm as jys,cpdm as zqdm,decode(bz, 2, cast(dryk as double) * "|| l_hlcsHKD ||", 3, cast(dryk as double) * "|| l_hlcsUSD ||", cast(dryk as double) ) as dryk,'' as zqlb,'fp' as lb,decode(bz, 2, cast(zxsz-tbcccb as double) *" || l_hlcsHKD ||", 3, cast(zxsz-tbcccb as double) * " || l_hlcsUSD 
                ||", cast(zxsz-tbcccb as double)) as fdyk,decode(bz, 2, cast(zxsz as double) * "|| l_hlcsHKD ||", 3,  cast(zxsz as double) * "|| l_hlcsUSD ||", cast(zxsz as double)) as zxsz"||
                " from "|| F_IDS_GET_TABLENAME('sparkFPcpfe', I_KHH)||" union ALL"||
                " select khh,jrjgdm as jys,cpdm as zqdm,decode(bz, 2, cast(dryk as double) * "|| l_hlcsHKD ||", 3, cast(dryk as double) * "|| l_hlcsUSD ||", cast(dryk as double) ) as dryk,'' as zqlb,'fp' as lb,0.0 as fdyk,0.0 as zxsz"||
                " from "|| F_IDS_GET_TABLENAME('sparkFPtzsy', I_KHH) ||" union ALL"||
                " select khh,jys,zqdm,decode(bz, 2, cast(dryk as double) * "|| l_hlcsHKD ||", 3, cast(dryk as double) * "|| l_hlcsUSD ||", cast(dryk as double) ) as dryk,zqlb,'rzrq' as lb,decode(bz, 2, cast(zxsz-tbcccb as double) *" || l_hlcsHKD ||", 3, cast(zxsz-tbcccb as double) * " || l_hlcsUSD 
                ||", cast(zxsz-tbcccb as double)) as fdyk,decode(bz, 2, cast(zxsz as double) * "|| l_hlcsHKD ||", 3,  cast(zxsz as double) * "|| l_hlcsUSD ||", cast(zxsz as double)) as zxsz"||
                " from "|| F_IDS_GET_TABLENAME('sparkXYzqye', I_KHH) ||" union ALL"|| 
                " select khh,jys,zqdm,decode(bz, 2, cast(dryk as double) * "|| l_hlcsHKD ||", 3, cast(dryk as double) * "|| l_hlcsUSD ||", cast(dryk as double) ) as dryk,zqlb,'rzrq' as lb,0.0 as fdyk,0.0 as zxsz"||
                " from "||F_IDS_GET_TABLENAME('sparkXYtzsy', I_KHH) ||" union ALL"||
                " select khh,jys,hydm as zqdm,decode(bz, 2, cast(dryk as double) * "|| l_hlcsHKD ||", 3, cast(dryk as double) * "|| l_hlcsUSD ||", cast(dryk as double) ) as dryk,'' as zqlb,'so' as lb,decode(bz, 2, cast(case when ccfx='2' then (tbcccb + zxsz) else (zxsz-tbcccb) end as double) *" || l_hlcsHKD ||", 3, cast(case when ccfx='2' then (tbcccb + zxsz) else (zxsz-tbcccb) end as double) * " || l_hlcsUSD 
                ||", cast(case when ccfx='2' then (tbcccb + zxsz) else (zxsz-tbcccb) end as double) as fdyk,decode(bz, 2, cast(zxsz as double) * "|| l_hlcsHKD ||", 3,  cast(zxsz as double) * "|| l_hlcsUSD ||", cast(zxsz as double)) as zxsz"||
                " from "||F_IDS_GET_TABLENAME('sparkSoHycc', I_KHH) ||" union ALL"||
                " select khh,jys,hydm as zqdm,decode(bz, 2, cast(dryk as double) * "|| l_hlcsHKD ||", 3, cast(dryk as double) * "|| l_hlcsUSD ||", cast(dryk as double) ) as dryk,'' as zqlb,'so' as lb,0.0 as fdyk,0.0 as zxsz"||
                " FROM "|| F_IDS_GET_TABLENAME('sparkSoTzsy', I_KHH)|| " )z"|| 
                " left join "|| F_IDS_GET_TABLENAME('sparkZqlb0', I_KHH) ||" a  on (z.zqlb=a.zqlb and z.jys=a.jys) "||
                " left join "||  F_IDS_GET_TABLENAME('sparkZcpz', I_KHH) ||"  b on (z.zqdm=b.zqdm and z.jys=b.jys)";
    /**
     * 调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
     */
                F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName)
  END;
  
  BEGIN
          l_tableName := l_dbname || 'sparkKhyk' || l_suffix;
          l_sqlBuf := "select khh,"||
                      " sum(case when lb='jzjy' then fdyk else 0 end) as fdyk_jzjy,"||
                      " sum(case when lb='fp' then fdyk else 0 end) as fdyk_jrcp,"||
                      " sum(case when lb='rzrq' then fdyk else 0 end) as fdyk_rzrq,"||
                      " sum(case when lb='so' then fdyk else 0 end) as fdyk_ggqq,"||
                      " sum(case when lb='fp' then dryk else 0 end) as yk_jrcp"||
                      " from "|| F_IDS_GET_TABLENAME('sparkTzfb', I_KHH) ||" group by khh";
     /**
     * 调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
     */
                F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName)
  END ;

    
  BEGIn
         SELECT nvl(cast(zxj as double),0) zxj,nvl (cast(zzl as double),0) zzl INTO hs300zxj,hs300zzl  from (select  
         row_number() over(partition by zsdm order by rq desc) rn,  nvl(zxj, zsp) as zxj, 
         zxj/lag(zxj, 1, zsp) over(partition by zsdm order by rq)-1 as zzl 
         from info.this_zshq where zsdm='399300' and rq between l_lastDay and I_RQ) a where rn=1;
  END;
 
  --------------------------------------- //1、集中交易资产-----------------------------------------------  
  BEGIN
    l_tableName := l_dbname || 'sparkRzd_0' || l_suffix;
    l_sqlBuf :=     "select  khh,"||
                    " cast(0 AS decimal(16,2)) as srzzc,"||
                    " cast(0 AS decimal(16,2)) as srfe,"||
                    " cast(0 AS decimal(9,4)) as srjz,"||
                    " cast(dryk AS decimal(16,2)) as yk,"||
                    " cast(dryk AS decimal(16,2)) as yk_by,"||
                    " cast(dryk AS decimal(16,2)) as yk_bn,"||
                    " cast(crje AS decimal(16,2)),"||
                    " cast(qcje AS decimal(16,2)),"||
                    " cast((lxsr+zrzj_fotc+zrsz) AS DECIMAL(16,2)) as zrzqsz,"||
                    " cast((zczj_totc+lxzc_gpzy+jyfy_gpzy+lxzc_qt+zcsz) AS DECIMAL(16,2)) as zczqsz,"||
                    " cast(zzc AS decimal(22,2)) as zzc_jzjy,"||
                    " cast(zqsz+zqsz_flt+zqsz_dyp AS decimal(22,2)) as zqsz_jzjy,"||
                    " cast(zjye AS decimal(16,2)) as zjye_jzjy,"||
                    " cast(dryk  AS decimal(16,2)) as yk_jzjy,"||
                    " cast(zfz  AS decimal(16,2)) as zfz_jzjy,"||
                    " cast(tzzh_zxjz AS decimal(9,4)) as zxjz_jzjy,"||
                    " cast(if(tzzh_zxjz_sr=0,0 ,(tzzh_zxjz-tzzh_zxjz_sr)/tzzh_zxjz_sr) AS decimal(9,4)) as zxjz_zzl_jzjy,"||
                    " cast(0 AS decimal(22,2)) as zzc_rzrq,"||
                    " cast(0 AS decimal(22,2)) as zqsz_rzrq,"||
                    " cast(0 AS decimal(16,2)) as zjye_rzrq,"||
                    " cast(0 AS decimal(16,2)) as zfz_rzrq,"||
                    " cast(0 AS decimal(16,2)) as yk_rzrq,"||
                    " cast(0 AS decimal(9,4)) as zxjz_rzrq,"||
                    " cast(0 AS decimal(9,4)) as zxjz_zzl_rzrq,"||
                    " cast(zqsz_jrcp AS decimal(16,2)) as zqsz_jrcp,"||
                    " cast(0 AS decimal(16,2)) as zzc_ggqq,"||
                    " cast(0 AS decimal(16,2)) as zjye_ggqq,"||
                    " cast(0 AS decimal(16,2)) as zqsz_ggqq,"||
                    " cast(0 AS decimal(16,2)) as yk_ggqq,"||
                    " cast(0 AS decimal(9,4)) as zxjz_ggqq,"||
                    " cast(0 AS decimal(9,4)) as zxjz_zzl_ggqq"||
                    " FROM " || F_IDS_GET_TABLENAME('sparkJzjyZc', I_KHH) || " T";    
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
      
 ------------------------------------//两融资产----------------------------------------------------
  BEGIN
    l_sqlBuf :=     "select  khh,"||
                    " cast(0 AS decimal(16,2)) as srzzc,"||
                    " cast(0 AS decimal(16,2)) as srfe," ||
                    " cast(0 AS decimal(9,4)) as srjz,"||
                    " dryk as yk,"||
                    " dryk as yk_by,"||
                    " dryk as yk_bn,"||
                    " crje,"||
                    " qcje,"||
                    " lxsr+zrsz as zrzqsz,"||
                    " lxzc_qt+zcsz as zczqsz,"||
                    " cast(0 AS decimal(22,2)) as zzc_jzjy,"||
                    " cast(0 AS decimal(22,2)) as zqsz_jzjy,"||
                    " cast(0 AS decimal(16,2)) as zjye_jzjy,"||
                    " cast(0 AS decimal(16,2)) as yk_jzjy,"||
                    " cast(0 AS decimal(16,2)) as zfz_jzjy,"||
                    " cast(0 AS decimal(9,4)) as zxjz_jzjy,"||
                    " cast(0 AS decimal(9,4)) as zxjz_zzl_jzjy,"||
                    " zzc as zzc_rzrq,"||
                    " zqsz as zqsz_rzrq,"||
                    " zjye as zjye_rzrq,"||
                    " zfz as zfz_rzrq,"||
                    " dryk as yk_rzrq," ||
                    " tzzh_zxjz as zxjz_rzrq,"||
                    " if(tzzh_zxjz_sr=0,cast(0 AS decimal(16,2)),(tzzh_zxjz-tzzh_zxjz_sr)/tzzh_zxjz_sr) as zxjz_zzl_rzrq,"||
                    " cast(0 AS decimal(16,2)) as zqsz_jrcp,"||
                    " cast(0 AS decimal(16,2)) as zzc_ggqq,"||
                    " cast(0 AS decimal(16,2)) as zjye_ggqq,"||
                    " cast(0 AS decimal(16,2)) as zqsz_ggqq,"||
                    " cast(0 AS decimal(16,2)) as yk_ggqq,"||
                    " cast(0 AS decimal(9,4)) as zxjz_ggqq,"||
                    " cast(0 AS decimal(9,4)) as zxjz_zzl_ggqq"||
                "  FROM " || F_IDS_GET_TABLENAME('sparkXyZc', I_KHH);                 
    --F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
 -- 数据插入至资产统计临时表sparkStatDR
    l_sql := 'INSERT INTO TABLE ' || F_IDS_GET_TABLENAME('sparkRzd_0', I_KHH) || l_columns || l_sqlBuf;
    EXECUTE IMMEDIATE l_sql;
  END;     
  
   ------------------------------------//个股期权资产----------------------------------------------------
  BEGIN
    l_sqlBuf :=     "select   khh,"||
                    " cast(0 AS decimal(16,2)) as srzzc,"||
                    " cast(0 AS decimal(16,2)) as srfe,"||
                    " cast(0 AS decimal(9,4)) as srjz,"||
                    " dryk as yk,"||
                    " dryk as yk_by,"||
                    " dryk as yk_bn,"||
                    " crje,"||
                    " qcje,"||
                    " lxsr+zrsz as zrzqsz,"||
                    " lxzc+zcsz as zczqsz,"||
                    " cast(0 AS decimal(22,2)) as zzc_jzjy,"||
                    " cast(0 AS decimal(22,2)) as zqsz_jzjy,"||
                    " cast(0 AS decimal(16,2)) as zjye_jzjy,"||
                    " cast(0 AS decimal(16,2)) as yk_jzjy,"||
                    " cast(0 AS decimal(16,2)) as zfz_jzjy,"||
                    " cast(0 AS decimal(9,4)) as zxjz_jzjy,"||
                    " cast(0 AS decimal(9,4)) as zxjz_zzl_jzjy,"||
                    " cast(0 AS decimal(22,2)) as zzc_rzrq,"||
                    " cast(0 AS decimal(22,2)) as zqsz_rzrq,"||
                    " cast(0 AS decimal(16,2)) as zjye_rzrq,"||
                    " cast(0 AS decimal(16,2)) as zfz_rzrq,"||
                    " cast(0 AS decimal(16,2)) as yk_rzrq,"||
                    " cast(0 AS decimal(9,4)) as zxjz_rzrq,"||
                    " cast(0 AS decimal(9,4)) as zxjz_zzl_rzrq,"||
                    " cast(0 AS decimal(16,2)) as zqsz_jrcp,"||
                    " jzc as zzc_ggqq,"||
                    " zjye as zjye_ggqq,"||
                    " zqsz-2*zqsz_kt as zqsz_ggqq,"||
                    " dryk as yk_ggqq,"||
                    " tzzh_zxjz as zxjz_ggqq,"||
                    " if(tzzh_zxjz_sr=0,cast(0 AS decimal(16,2)),(tzzh_zxjz-tzzh_zxjz_sr)/tzzh_zxjz_sr) as zxjz_zzl_ggqq"||
                "  FROM " || F_IDS_GET_TABLENAME('sparkSoZc', I_KHH);                 
    --F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
 -- 数据插入至资产统计临时表sparkStatDR
    l_sql := 'INSERT INTO TABLE ' || F_IDS_GET_TABLENAME('sparkRzd_0', I_KHH) || l_columns || l_sqlBuf;    
    EXECUTE IMMEDIATE l_sql;
  END; 
  
  
   
   ------------------------------------//上日账单----------------------------------------------------
  BEGIN
    l_sqlBuf :=     " select  khh,"||
                    " zzc as srzzc,"||
                    " zxfe as srfe,"||
                    " zxjz as srjz,"||
                    " cast(0 AS decimal(16,2)) as yk,"||
IF (l_isMonthBegin, " cast(0 AS decimal(16,2)) AS yk_by, ", " yk_by as yk_by, ") ||
IF (l_isYearBegin, " cast(0 AS decimal(16,2)) as yk_bn, ", " yk_bn as yk_bn,") ||
                    " cast(0 AS decimal(16,2)) as crje,"||
                    " cast(0 AS decimal(16,2)) as qcje,"||
                    " cast(0 AS decimal(16,2)) as zrzqsz,"||
                    " cast(0 AS decimal(16,2)) as zczqsz,"||
                    " cast(0 AS decimal(22,2)) as zzc_jzjy,"||
                    " cast(0 AS decimal(22,2)) as zqsz_jzjy,"||
                    " cast(0 AS decimal(16,2)) as zjye_jzjy,"||
                    " cast(0 AS decimal(16,2)) as yk_jzjy,"||
                    " cast(0 AS decimal(16,2)) as zfz_jzjy,"||
                    " cast(0 AS decimal(9,4)) as zxjz_jzjy,"||
                    " cast(0 AS decimal(9,4)) as zxjz_zzl_jzjy,"||
                    " cast(0 AS decimal(22,2)) as zzc_rzrq,"||
                    " cast(0 AS decimal(22,2)) as zqsz_rzrq,"||
                    " cast(0 AS decimal(16,2)) as zjye_rzrq,"||
                    " cast(0 AS decimal(16,2)) as zfz_rzrq," ||
                    " cast(0 AS decimal(16,2))as yk_rzrq,"||
                    " cast(0 AS decimal(9,4)) as zxjz_rzrq,"||
                    " cast(0 AS decimal(9,4)) as zxjz_zzl_rzrq,"||
                    " cast(0 AS decimal(22,2)) as zqsz_jrcp,"||
                    " cast(0 AS decimal(16,2)) as zzc_ggqq,"||
                    " cast(0 AS decimal(16,2)) as zjye_ggqq,"||
                    " cast(0 AS decimal(16,2)) as zqsz_ggqq,"||
                    " cast(0 AS decimal(16,2)) as yk_ggqq,"||
                    " cast(0 AS decimal(9,4)) as zxjz_ggqq,"||
                    " cast(0 AS decimal(9,4)) as zxjz_zzl_ggqq"||
                "  FROM " || F_IDS_GET_TABLENAME('sparkSrzd', I_KHH);                 
    --F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
 -- 数据插入至资产统计临时表sparkStatDR
    l_sql := 'INSERT INTO TABLE ' || F_IDS_GET_TABLENAME('sparkRzd_0', I_KHH) || l_columns || l_sqlBuf; 
    EXECUTE IMMEDIATE l_sql;
  END; 
  
           BEGIN
  ----------------------------------------/sparkJzjyZcszDryk-----------------------------------------------------
    /**
     * 临时表名
     * 需要添加相关前缀：
     */
    l_tableName := l_dbname || 'sparkJzjyZcszDryk' || l_suffix;
    l_sqlBuf :=  " SELECT  KHH,"||
                 " CAST(SUM(decode(bz, 2, cast(ZCSZ_DRYK as double) * "|| l_hlcsHKD ||", 3, cast(ZCSZ_DRYK as double) * "|| l_hlcsUSD || ", cast(ZCSZ_DRYK as double))) AS DECIMAL(16,2)) AS ZCSZ_DRYK"||
                 " FROM (SELECT  T.KHH,"||
                 " T.BZ, CASE "||
                 "  WHEN A5.PARAM_KIND IS NOT NULL AND T.YSSL<0 AND T.CJJE=0 AND Q.KHH IS NOT NULL THEN Q.DRYK"||
                 "  WHEN T.JYLB='21' AND (T.ZQLB NOT LIKE 'A%' OR T.ZQLB='A0') AND T.NOTE NOT LIKE '%债券兑付过出%' AND BG.ZQDM IS NULL AND T.YSSL<0 AND T.YSJE>0 AND T.CJJE=0 AND Q.KHH IS NOT NULL THEN"||
                 "     Q.DRYK"||
                 "  when T.jylb='19' and T.note like '%撤销指定证券转出%' AND T.YSSL<0 AND T.CJJE=0 AND Q.KHH IS NOT NULL THEN "||
                 "      Q.DRYK "||
                 " when T.jylb='19' and T.jys='5' and T.note like '%托管转出%' AND T.YSSL<0 AND T.CJJE=0 AND Q.KHH IS NOT NULL THEN"||
                 "      Q.DRYK"||
                 "  ELSE 0  END AS ZCSZ_DRYK"||
                 " FROM " ||  F_IDS_GET_TABLENAME('sparkJzjyJgls', I_KHH) ||" T "||
                 " LEFT JOIN " ||F_IDS_GET_TABLENAME('sparkParamValue', I_KHH)||"  A5" || 
                 " ON (A5.PARAM_KIND = '05' AND A5.PARAM_VALUE = T.JYLB) "||
                 " LEFT JOIN "||F_IDS_GET_TABLENAME('sparkZqdmbg', I_KHH)|| " BG"|| 
                 " ON (T.JYS=BG.JYS AND T.ZQDM=BG.ZQDM) "||
                 " LEFT JOIN "||F_IDS_GET_TABLENAME('sparkJzjyTzsy', I_KHH)|| "  Q" ||
                 " ON (T.KHH=Q.KHH AND T.GDH=Q.GDH AND T.JYS=Q.JYS AND T.ZQDM=Q.ZQDM)) A"||
                " GROUP BY KHH";
    /**
     * 调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
     */
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName)
  END;
  
  
   BEGIN
  ----------------------------------------/sparkXyZcszDryk-----------------------------------------------------
    /**
     * 临时表名
     * 需要添加相关前缀：
     */
    l_tableName := l_dbname || 'sparkXyZcszDryk' || l_suffix;
    l_sqlBuf :=  " SELECT   KHH,"||
                 " CAST(ROUND(SUM(decode(bz, 2, cast(ZCSZ_DRYK as double) * "|| l_hlcsHKD ||", 3, cast(ZCSZ_DRYK as double) * "|| l_hlcsUSD || ", cast(ZCSZ_DRYK as double))),2) AS DECIMAL(16,2)) AS ZCSZ_DRYK"||
                " FROM(SELECT  T.KHH,"||
                " T.BZ, "||
                " CASE  WHEN T.JYLB = '66' OR (T.JYLB='21' AND T.NOTE LIKE '%证券调出%') THEN "||
                "      Q.DRYK"||
                " ELSE 0 END AS ZCSZ_DRYK"||
                " FROM "||F_IDS_GET_TABLENAME('sparkXyJgls', I_KHH) ||" T "||
                " LEFT JOIN "|| F_IDS_GET_TABLENAME('sparkXyTzsy', I_KHH)||"  Q "||
                " ON (T.KHH=Q.KHH AND T.GDH=Q.GDH AND T.JYS=Q.JYS AND T.ZQDM=Q.ZQDM)) A"||
                " GROUP BY KHH";
    /**
     * 调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
     */
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName)
  END;
  
  BEGIN
  ----------------------------------------/sparkRzd_1-----------------------------------------------------
    /**
     * 临时表名
     * 需要添加相关前缀：
     */
    l_tableName := l_dbname || 'sparkRzd_1' || l_suffix;
    l_sqlBuf :=  " select   khh,"||
                 " sum(srzzc) as srzzc,"||
                 " sum(srfe) as srfe,"||
                 " sum(srjz) as srjz,"||
                 " sum(zzc_jzjy+zzc_rzrq+zzc_ggqq) as zzc,"||
                 " sum(zjye_jzjy+zjye_rzrq+zjye_ggqq) as zjye,"||
                 " sum(zqsz_jzjy+zqsz_rzrq+zqsz_ggqq) as zqsz,"||
                 " sum(zfz_jzjy+zfz_rzrq) as zfz,"||
                 " sum(yk) as yk,"||
                 " sum(yk_by) as yk_by,"||
                 " sum(yk_bn) as yk_bn,"||
                 " sum(crje) as crje,"||
                 " sum(qcje) as qcje,"||
                 " sum(zrzqsz) as zrzqsz,"||
                " sum(zczqsz) as zczqsz,"||
                " sum(zzc_jzjy) as zzc_jzjy,"||
                " sum(zqsz_jzjy) as zqsz_jzjy,"||
                " sum(zjye_jzjy) as zjye_jzjy," ||
                " sum(yk_jzjy) as yk_jzjy," ||
                " sum(zfz_jzjy) as zfz_jzjy," ||
                " sum(zxjz_jzjy) as zxjz_jzjy,"||
                " sum(zxjz_zzl_jzjy) as zxjz_zzl_jzjy,"||
                " sum(zzc_rzrq) as zzc_rzrq,"||
                " sum(zqsz_rzrq) as zqsz_rzrq,"||
                " sum(zjye_rzrq) as zjye_rzrq,"||
                " sum(zfz_rzrq) as zfz_rzrq,"||
                " sum(yk_rzrq) as yk_rzrq,"||
                " sum(zxjz_rzrq) as zxjz_rzrq,"||
                " sum(zxjz_zzl_rzrq) as zxjz_zzl_rzrq,"||
                " sum(zqsz_jrcp) as zqsz_jrcp,"||
                " sum(zzc_ggqq) as zzc_ggqq,"||
                " sum(zjye_ggqq) as zjye_ggqq,"||
                " sum(zqsz_ggqq) as zqsz_ggqq,"||
                " sum(yk_ggqq) as yk_ggqq,"||
                " sum(zxjz_ggqq) as zxjz_ggqq,"||
                " sum(zxjz_zzl_ggqq) as zxjz_zzl_ggqq"||
                " FROM "||F_IDS_GET_TABLENAME('sparkRzd_0', I_KHH)||
                " group by khh";
    /**
     * 调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
     */
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName)
  END;
  
  
   BEGIN
  ----------------------------------------/sparkRzd_2-----------------------------------------------------
    /**
     * 临时表名
     * 需要添加相关前缀：
     */
    l_tableName := l_dbname || 'sparkRzd_2' || l_suffix;
    l_sqlBuf :=     "select  t.khh,"||
                    " srjz,"||
                    " zzc,"||
                    " zjye,"||
                    " zqsz,"||
                    " zfz,"||
                    " yk,"||
                    " decode(srzzc+(crje+zrzqsz) - (qcje+zczqsz),0,0,zzc/(srzzc+(crje+zrzqsz) - (qcje+zczqsz))-1) as ykl,"||
                    " yk_by,"||
                    " yk_bn,"||
                    " crje,"||
                    " qcje,"||
                    " zrzqsz,"||
                    " zczqsz,"||
                    " (crje+zrzqsz) - (qcje+zczqsz) as zcjlr,"||
                    " case when nvl(srfe, 0.0)=0 then  "||
                    " zzc-zfz     "||
                    " else  "||
                    " srfe +  ((crje+zrzqsz) - (qcje+zczqsz-nvl(jd.zcsz_dryk,0)-nvl(xd.zcsz_dryk,0))/nvl(srjz,1.0))"||
                    " end as zxfe,"||
                    " zzc_jzjy,"||
                    " zqsz_jzjy,"||
                    " zjye_jzjy,"||
                    " yk_jzjy,"||
                    " zfz_jzjy,"||
                    " zxjz_jzjy,"||
                    " zxjz_zzl_jzjy,"||
                    " zzc_rzrq,"||
                    " zqsz_rzrq,"||
                    " zjye_rzrq,"||
                    " zfz_rzrq,"||
                    " yk_rzrq,"||
                    " zxjz_rzrq,"||
                    " zxjz_zzl_rzrq,"||
                    " zqsz_jrcp,"||
                    " zzc_ggqq,"||
                    "  zjye_ggqq,"||
                    " zqsz_ggqq,"||
                    " yk_ggqq,"||
                    " zxjz_ggqq,"||
                    " zxjz_zzl_ggqq"||
                    " FROM "||F_IDS_GET_TABLENAME('sparkRzd_1', I_KHH)||" t "||
                    " left join "|| F_IDS_GET_TABLENAME('sparkJzjyZcszDryk', I_KHH) ||" jd on (t.khh=jd.khh) "||
                    " left join "|| F_IDS_GET_TABLENAME('sparkXyZcszDryk', I_KHH) || " xd on (t.khh=xd.khh)";
    /**
     * 调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
     */
     F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName)
  END;
  ---------------------------------------集中交易指标生成----------------------------------------------------
  begin
    l_tableName := F_IDS_GET_TABLENAME('sparkJZJYZcfbzc', I_KHH);
    l_sqlBuf := "SELECT
                T.khh
                 ,  concat('\"secu_cptl_bal\":'  --资金余额
                           , concat('\"'
                                     , cast(nvl(zjye, 0.0) AS STRING)
                                     , '\"')
                           , ','
                           , '\"secu_roug_ast\":'   --在途资产
                           , concat('\"'
                                     , sum(CASE WHEN B.xzlb IN (3,11,12,13,14,15,21,22,23,24) THEN
                                        B.xzje
                                        ELSE
                                        0 end)
                                     , '\"')
                           , ','
                           , '\"secu_amnd_amt\":'  --修正资金
                           , concat('\"'
                                     , sum(CASE WHEN B.xzlb = 31 THEN
                                       B.xzje
                                        ELSE
                                        0 end)
                                     , '\"')
                            , ','
                            , '\"secu_trea_repo_mval\":'  --国债逆回购未到期
                            , concat('\"'
                                     , sum(CASE WHEN B.xzlb = 3 THEN
                                        B.xzje
                                        ELSE
                                        0 end)
                                     , '\"')
                            , ','
                            , '\"secu_amnd_liab\":'  --修正负债
                            , concat('\"'
                                     , sum(CASE WHEN B.xzlb IN (2,5) THEN
                                        B.xzje
                                        ELSE
                                        0 end)
                                     , '\"')
                            , ','
                            , '\"secu_amnd_mval\":'  --修正市值
                            , concat('\"'
                                     , sum(CASE WHEN B.xzlb IN (1, 4, 32, 33,34) THEN
                                        B.xzje
                                        ELSE
                                        0 end)
                                     , '\"')
                                     ) as zcfb
            FROM
                " || F_IDS_GET_TABLENAME('sparkJzjyZc', I_KHH) || " T
                LEFT JOIN (SELECT
                                khh
                                , xzlb
                                 , sum(DECODE(BZ, '2', " || l_hlcsHKD || ", '3', " || l_hlcsUSD || ", 1) * xzje ) AS xzje
                            FROM
                                DSC_STAT.t_stat_khzcxzmx
                            WHERE
                                 ksrq = " || I_RQ || " AND jsrq <=  " || I_RQ || "
                                 AND ZHLB = 1
                            GROUP BY
                                 khh, xzlb) B
                ON (T.khh = B.khh) GROUP BY T.khh, T.zjye";
                
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
    
    --股票市值/债券等
    l_tableName := F_IDS_GET_TABLENAME('sparkJZJYZcfbcc', I_KHH);
    l_sqlBuf := "SELECT
                KHH
                 , concat(
                 --股票类
                          '\"secu_stk_mval\"\:' --股票市值1100-2499
                           , concat('\"'
                                     , sum(CASE WHEN substr(B.zqpz, 1, 1) IN (1, 2) THEN
                                                 T.zxsz
                                            ELSE
                                                0
                                            END)
                                     , '\"')
                           , ','
                           , '\"secu_stk_vol\"\:' --股票数量/持仓数量
                           , concat('\"'
                                     , sum(CASE WHEN substr(B.zqpz, 1, 1) IN (1, 2) THEN
                                                 T.zqsl
                                            ELSE
                                                0
                                            END)
                                     , '\"')
                           , ','
                           , '\"secu_ifnd_mval\"\:' --场内基金市值2400-2499
                           , concat('\"'
                                     , sum(CASE WHEN substr(B.zqpz, 1, 2) = 24 THEN
                                                 T.zxsz
                                            ELSE
                                                0
                                            END)
                                     , '\"')
                           , ','
                           ,
                           --债券类
                             '\"secu_bond_mval\"\:' --债券市值4100-4499
                           , concat('\"'
                                     , sum(CASE WHEN substr(B.zqpz, 1, 1) = 4 THEN
                                                 T.zxsz
                                            ELSE
                                                0
                                            END)
                                     , '\"')
                           , ','
                           , '\"secu_bond_vol\"\:' --债券数量
                           , concat('\"'
                                     , sum(CASE WHEN substr(B.zqpz, 1, 1) = 4 THEN
                                                 T.zqsl
                                            ELSE
                                                0
                                            END)
                                     , '\"')
                           , ','
                           , '\"secu_cvtb_mval\"\:' --可转债市值
                           , concat('\"'
                                     , sum(CASE WHEN B.zqpz = 4401 THEN
                                                 T.zxsz
                                            ELSE
                                                0
                                            END)
                                     , '\"')
                           , ','
                           , '\"secu_cvtb_vol\"\:' --可转债数量
                           , concat('\"'
                                     , sum(CASE WHEN B.zqpz = 4401 THEN
                                                 T.zqsl
                                            ELSE
                                                0
                                            END)
                                     , '\"')) AS zcfb
            FROM
                " || F_IDS_GET_TABLENAME('sparkJzjyZqye', I_KHH) || " T
                LEFT JOIN " || F_IDS_GET_TABLENAME('sparkzqpzDy', I_KHH) || " B
                ON (T.JYS = B.JYS AND T.ZQLB = B.ZQLB)
            GROUP BY
                 khh";
                 
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
    
    --金融产品
    l_tableName := F_IDS_GET_TABLENAME('sparkFpZcfb', I_KHH);
    l_sqlBuf := "select
                    khh
                     , concat('\"secu_cash_bao\"\:'  --现金宝
                               , concat('\"'
                                         , sum(case when cpdm='A72001' THEN -- zqpz = 2510
                                                     zxsz
                                                else
                                                    0
                                                end)
                                         , '\"')
                               , ','
                               ,'\"secu_cash_bao_inc\"\:'  --现金宝累计收益
                               , concat('\"'
                                         , sum(case when cpdm='A72001' THEN -- zqpz = 2510
                                                     ljyk
                                                else
                                                    0
                                                end)
                                         , '\"')
                               , ','
                               , '\"secu_finl_pd_mval\"\:'  --金融产品市值
                               , concat('\"'
                                         , sum(case when cpdm <> 'A72001' AND substr(zqpz, 1, 2) in (25,26,72, 73, 74, 75, 76) then
                                                     zxsz
                                                else
                                                    0
                                                end)
                                         , '\"')
                               , ','
                               , '\"secu_finl_pd_vol\"\:'  --金融产品份额
                               , concat('\"'
                                         , sum(case when  cpdm <> 'A72001' AND substr(zqpz, 1, 2) in (25,26,72, 73, 74, 75, 76)
                                                then
                                                     cpsl
                                                else
                                                    0
                                                end)
                                         , '\"')
                               , ','
                               , '\"secu_payf_vou_mval\"\:'  --收益凭证市值
                               , concat('\"'
                                         , sum(case when zqpz = 7205 then
                                                     zxsz
                                                else
                                                    0
                                                end)
                                         , '\"')
                               , ','
                               , '\"secu_bnk_fm_mval\"\:'  --银行理财市值
                               , concat('\"'
                                         , sum(case when zqpz = 7301 then
                                                     zxsz
                                                else
                                                    0
                                                end)
                                         , '\"')
                               , ','
                               , '\"secu_pte_mval\"\:'  --私募市值
                               , concat('\"'
                                         , sum(case when zqpz = 2601 then
                                                     zxsz
                                                else
                                                    0
                                                end)
                                         , '\"')
                               , ','
                               , '\"secu_ofnd_mval\"\:' --场外开基市值
                               , concat('\"'
                                         , sum(case when  cpdm <> 'A72001' and substr(zqpz, 1, 2) = 25 then
                                                     zxsz
                                                else
                                                    0
                                                end)
                                         , '\"')
                               , ','
                               , '\"secu_ofnd_vol\"\:'  --场外开基份额
                               , concat('\"'
                                         , sum(case when  cpdm <> 'A72001' and substr(zqpz, 1, 2) = 25 then
                                                     cpsl
                                                else
                                                    0
                                                end)
                                         , '\"')) as zcfb
                from
                    "|| F_IDS_GET_TABLENAME('sparkFpCpfeGt', I_KHH) || "
                group by
                     khh";
                     
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
    
    l_tableName := F_IDS_GET_TABLENAME('sparkJZJYZcfbAll', I_KHH);
    l_sqlBuf := "select khh, " ||
               " concat_ws(',', collect_set(zcfb)) as jzjy_zcfb " ||
               " from (" ||
               " select khh, zcfb from " ||
               F_IDS_GET_TABLENAME('sparkJZJYZcfbzc', I_KHH) || 
               " UNION ALL " ||
               "SELECT KHH, ZCFB FROM " ||
               F_IDS_GET_TABLENAME('sparkJZJYZcfbcc', I_KHH) ||
               " UNION ALL " ||
               "SELECT KHH, ZCFB FROM " ||
               F_IDS_GET_TABLENAME('sparkFpZcfb', I_KHH) ||
               " ) T GROUP BY KHH";
    
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
    
  end;
  ---------------------------------------信用指标生成--------------------------------------------------------
  begin
    l_tableName := F_IDS_GET_TABLENAME('sparkXYZcfbzc', I_KHH);
    l_sqlBuf := "SELECT
                T.khh
                 , concat('\"marg_cptl_bal\"\:'   --资金余额
                           , concat('\"',cast(nvl(zjye, 0.0) AS STRING),'\"')
                           , ','
                           , '\"marg_tot_liab\"\:' --信用总负债
                           , concat('\"', cast(nvl(zfz, 0.0) AS STRING), '\"')
                           , ','
                           , '\"marg_fin_liab\"\:' --融资负债
                           , concat('\"',cast(nvl(rzfz, 0.0) AS STRING), '\"')
                           , ','
                           , '\"marg_shts_liab\"\:'  --融券负债
                           , concat('\"',cast(nvl(rqfz, 0.0) AS STRING), '\"')
                           , ','
                           , '\"marg_oth_liab\"\:' --其他负债
                           , concat('\"', cast(nvl((zfz - rzfz - rqfz), 0.0) AS STRING), '\"')
                           , ','
                           , '\"marg_roug_ast\":'   --在途资产
                           , concat('\"'
                                     , sum(CASE WHEN B.xzlb IN (3,11,12,13,14,15,21,22,23,24) THEN
                                        B.xzje
                                        ELSE
                                        0 end)
                                     , '\"')
                           , ','
                           , '\"marg_amnd_amt\":'  --修正资金
                           , concat('\"'
                                     , sum(CASE WHEN B.xzlb = 31 THEN
                                       B.xzje
                                        ELSE
                                        0 end)
                                     , '\"')
                            , ','
                            , '\"marg_amnd_mval\":'  --修正市值
                            , concat('\"'
                                     , sum(CASE WHEN B.xzlb IN (1, 4, 32,33, 34) THEN
                                        B.xzje
                                        ELSE
                                        0 end)
                                     , '\"')
                           ) AS xy_zcfb
            FROM
                " || F_IDS_GET_TABLENAME('sparkXyZc', I_KHH) || " T
                LEFT JOIN (SELECT
                                khh
                                , xzlb
                                 , sum(DECODE(BZ, '2', " || l_hlcsHKD || ", '3', " || l_hlcsUSD || ", 1) * xzje ) AS xzje
                            FROM
                                DSC_STAT.T_STAT_KHZCXZMX
                            WHERE
                                 ksrq = " || I_RQ || " AND jsrq <=  " || I_RQ || "
                                 AND ZHLB = 2
                            GROUP BY
                                 khh, xzlb) B
                ON (T.khh = B.khh) GROUP BY T.KHH, T.ZJYE, T.ZFZ, T.RZFZ, T.RQFZ";
                
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
    
    l_tableName := F_IDS_GET_TABLENAME('sparkXYZcfbcc', I_KHH);
    l_sqlBuf := "SELECT
                KHH
                 , concat(
                 --股票类
                          '\"marg_stk_mval\"\:'  --股票市值1100-2499
                           , concat('\"', sum(
                           CASE WHEN substr(B.zqpz, 1,1) IN (1,2) THEN
                                T.zxsz
                           ELSE
                                0
                            END 
                           ), '\"')
                           , ','
                           , '\"marg_stk_vol\"\:'  --股票数量/持仓数量
                           , concat('\"', sum(
                           CASE WHEN substr(B.zqpz, 1,1) IN (1,2) THEN
                                T.zqsl
                           ELSE
                                0
                            END
                            ), '\"')
                           , ','
                           , '\"marg_ifnd_mval\"\:' --场内基金市值2400-2499
                           , concat('\"', sum(
                           CASE WHEN substr(B.zqpz, 1,2) = 24  THEN
                                T.zxsz
                           ELSE
                                0
                            END ), '\"')
                           , ','
                           ,
                           --债券类
                             '\"marg_bond_mval\"\:'  --债券市值4100-4499
                           , concat('\"', sum(
                           CASE WHEN substr(B.zqpz, 1,1) = 4  THEN
                                T.zxsz
                           ELSE
                                0
                            END), '\"')
                           , ','
                           , '\"marg_bond_vol\"\:'  --债券数量
                           , concat('\"', sum(
                           CASE WHEN substr(B.zqpz, 1,1) = 4  THEN
                                T.zqsl
                           ELSE
                                0
                            END), '\"')
                           , ','
                           , '\"marg_cvtb_mval\"\:' --可转债市值
                           , concat('\"', sum(
                           CASE WHEN B.zqpz = 4401  THEN
                                T.zxsz
                           ELSE
                                0
                            END), '\"')
                           , ','
                           , '\"marg_cvtb_vol\"\:'
                           , concat('\"', sum(
                           CASE WHEN B.zqpz = 4401  THEN
                                T.zqsl
                           ELSE
                                0
                            END), '\"')
                           ) AS xy_zcfb
            FROM
                " || F_IDS_GET_TABLENAME('sparkXyZqye', I_KHH) || " T
                LEFT JOIN " || F_IDS_GET_TABLENAME('sparkzqpzDy', I_KHH) || " B
                ON (T.JYS = B.JYS AND T.ZQLB = B.ZQLB)
            GROUP BY
                 khh";
                 
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName)           
  end;
    
   BEGIN
  ----------------------------------------/sparkZcfbZqpz-----------------------------------------------------
    /*
     * 临时表名
     * 需要添加相关前缀：
     */
    l_tableName := l_dbname || 'sparkZcfbZqpz' || l_suffix;
    l_sqlBuf :=         " select khh,"||
                        " concat('{',concat_ws(',', jzjy_zcfb, xy_zcfbzc, xy_zcfbcc),'}') as zcfb_zqpz_list "||
                        " FROM  (select  T.khh,"||
                        " T.jzjy_zcfb,"||
                        " B.xy_zcfb as xy_zcfbzc, "||
                        " C.xy_zcfb as xy_zcfbcc "||
                        " from "|| F_IDS_GET_TABLENAME('sparkJZJYZcfbAll', I_KHH) ||
                        "  T left join " || F_IDS_GET_TABLENAME('sparkXYZcfbzc', I_KHH) ||
                        "  B on (T.khh = B.khh)    " ||                    
                        "  left join " || F_IDS_GET_TABLENAME('sparkXYZcfbcc', I_KHH) ||
                        "  C on (T.khh = C.khh) 
                         ) a";
    /**
     * 调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
     */
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName)
  END;
  
     BEGIN
  ----------------------------------------/sparkZcfbZclb-----------------------------------------------------
    /**
     * 临时表名
     * 需要添加相关前缀：
     */
    l_tableName := l_dbname || 'sparkZcfbZclb' || l_suffix;
    l_sqlBuf :=             " select khh,"||
                            " concat('{',concat_ws(',',collect_set(concat_ws('\:', concat('\"',zcpz,'\"'), concat('\"',cast(zbz as string),'\"')) )),'}') as zcfb_zclb_list"||
                            " from (select  khh,"||
                            " zcpz,"||
                            " cast(sum(zxsz) as decimal(16,2)) as zbz"||
                            " from "||F_IDS_GET_TABLENAME('sparkTzfb', I_KHH) ||" group by khh,zcpz"||
                            " union ALL "||
                            " select  khh,"||
                            " '准现金类'  as zcpz,"||
                            " cast(zzc-zqsz as decimal(16,2)) as zbz" ||
                            
                            " from "|| F_IDS_GET_TABLENAME('sparkRzd_2', I_KHH)||" where zzc-zqsz!=0) a group by khh";
    /**
     * 调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
     */
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName)
  END;
  
  
    BEGIN
  ----------------------------------------/sparkZqpzYl-----------------------------------------------------
    /**
     * 临时表名
     * 需要添加相关前缀：
     */
    l_tableName := l_dbname || 'sparkZqpzYl' || l_suffix;
    l_sqlBuf :=             " select   khh,"||
                            " concat('{',concat_ws(',',collect_set(concat_ws('\:', concat('\"',zqpz,'\"'), concat('\"',cast(dryk as string),'\"')) )),'}') as yl_zqpz_list"||
                            " from (select  khh,"||
                            " zqpz,"||
                            " sum(dryk) as dryk"||
                            " from "||F_IDS_GET_TABLENAME('sparkTzfb', I_KHH) ||" where dryk>0 group by khh,zqpz)a group by khh";
    /**
     * 调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
     */
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName)
  END;
  
   BEGIN
  ----------------------------------------/sparkZqpzKs-----------------------------------------------------
    /**
     * 临时表名
     * 需要添加相关前缀：
     */
    l_tableName := l_dbname || 'sparkZqpzKs' || l_suffix;
    l_sqlBuf :=             "select   khh,"||
                            " concat('{',concat_ws(',',collect_set(concat_ws('\:', concat('\"',zqpz,'\"'), concat('\"',cast(dryk as string),'\"')) )),'}') as ks_zqpz_list"||
                            " FROM (select  khh,"||
                            " zqpz,"||
                            " sum(dryk) as dryk"||
                            " from "||F_IDS_GET_TABLENAME('sparkTzfb', I_KHH) ||"  where dryk<0 group by khh,zqpz)a group by khh";
                            
    /**
     * 调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
     */
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName)
  END;
  
   BEGIN
  ----------------------------------------/sparkRzdResult-----------------------------------------------------
    /**
     * 临时表名
     * 需要添加相关前缀：rq, zfz_jzjy]
     */
    l_tableName := l_dbname || 'sparkRzdResult' || l_suffix;
    
    
    l_sqlBuf :=         " select  t.khh,"||
                        " zzc,"||
                        " zjye,"||
                        " zqsz,"||
                        " zfz,"||
                        " yk,"||
                        " ykl,"||
                        " yk_by,"||
                        " yk_bn,"||
                        " crje,"||
                        " qcje,"||
                        " zrzqsz,"||
                        " zczqsz,"||
                        " zcjlr,"||
                        " zxfe,"||
                        " decode(zxfe,0,1,zzc/zxfe)  as zxjz,"||
                        " nvl(decode(zxfe,0,0,zzc/zxfe/srjz-1),0) as zxjz_zzl, "||
                        nvl(hs300zxj,0)||" as zshq_hs300, "||--可能有问题--已处理
                        nvl(hs300zzl,0)||" as zshq_hs300_zzl, "||
                        " nvl(zq.zcfb_zqpz_list,'') as zcfb_zqpz_list,"||
                        " nvl(zc.zcfb_zclb_list,'') as zcfb_zclb_list,"||
                        " nvl(yl.yl_zqpz_list,'') as yl_zqpz_list,"||
                        " nvl(ks.ks_zqpz_list,'') as ks_zqpz_list,"||
                        " zzc_jzjy,"||
                        " zqsz_jzjy,"||
                        " zjye_jzjy,"||
                        " yk_jzjy,"||
                        " nvl(y.fdyk_jzjy,0) as fdyk_jzjy,"||
                        --" zfz_jzjy,"||
                        " zxjz_jzjy,"||
                        " zxjz_zzl_jzjy,"||
                        " zzc_rzrq,"||
                        " zqsz_rzrq,"||
                        " zjye_rzrq,"||
                        " zfz_rzrq,"|| 
                        " yk_rzrq,"||
                        " nvl(fdyk_rzrq,0) as fdyk_rzrq,"||
                        " zxjz_rzrq,"||
                        " zxjz_zzl_rzrq,"||
                        " zqsz_jrcp,"||
                        " nvl(yk_jrcp,0) as yk_jrcp,"||
                        " nvl(fdyk_jrcp,0) as fdyk_jrcp,"||
                        " zzc_ggqq,"||
                        " zjye_ggqq,"||
                        " zqsz_ggqq,"||
                        " yk_ggqq,"||
                        " nvl(fdyk_ggqq,0) as fdyk_ggqq,"||
                        " zxjz_ggqq,"||
                        " zxjz_zzl_ggqq,"||
                        I_RQ ||" AS  rq "|| 
                        " from " ||F_IDS_GET_TABLENAME('sparkRzd_2', I_KHH)||"  t "||
                        " left join " || F_IDS_GET_TABLENAME('sparkKhyk', I_KHH) || " y   on (t.khh=y.khh)"||
                        " left join "|| F_IDS_GET_TABLENAME('sparkZcfbZqpz', I_KHH)|| " zq on (t.khh=zq.khh)"||
                        " left join "|| F_IDS_GET_TABLENAME('sparkZcfbZclb', I_KHH) || " zc on (t.khh=zc.khh)" ||
                        " left join "|| F_IDS_GET_TABLENAME('sparkZqpzYl', I_KHH)|| "  yl on (t.khh=yl.khh)" ||
                        " left join "|| F_IDS_GET_TABLENAME('sparkZqpzKs', I_KHH) || " ks on (t.khh=ks.khh)";                   
    /**
     * 调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
     */
   F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName)
                
   F_IDS_OVERWRITE_PARTITION(l_tableName, 'cust', 't_stat_zd_r',I_RQ,I_KHH);
  END;
  BEGIN 
    SELECT f_get_jyr_date(date_format(SYSDATE, 'yyyyMMdd'), -1) INTO l_dataDay FROM system.dual;
    IF I_RQ = l_dataDay THEN
      l_sqlBuf := "INSERT INTO TABLE CUST.t_stat_zd_sr " ||
                " SELECT z.KHH , " ||
                "   NVL(k.khxm, cast('' AS string)) AS khxm , " ||
                "   NVL(k.khrq, cast(0 AS STRING)) AS khrq , " ||
                "   NVL(k.yyb, cast('' AS string)) AS yyb, " ||
                "   NVL(yybmc, cast('' AS string)) AS yybmc, " ||
                "   Z.zzc ,
                    Z.zjye ,
                    Z.zqsz ,
                    Z.zfz ,
                    Z.yk ,
                    Z.ykl ,
                    Z.yk_by ,
                    Z.yk_bn ,
                    Z.crje ,
                    Z.qcje ,
                    Z.zrzqsz ,
                    Z.zczqsz ,
                    Z.zcjlr ,
                    Z.zxfe ,
                    Z.zxjz ,
                    Z.zxjz_zzl ,
                    Z.zshq_hs300 ,
                    Z.zshq_hs300_zzl ,
                    Z.zcfb_zqpz_list ,
                    Z.zcfb_zclb_list ,
                    Z.yl_zqpz_list ,
                    Z.ks_zqpz_list ,
                    Z.zzc_jzjy ,
                    Z.zqsz_jzjy ,
                    Z.zjye_jzjy ,
                    Z.yk_jzjy ,
                    Z.fdyk_jzjy ,
                    Z.zxjz_jzjy ,
                    Z.zxjz_zzl_jzjy ,
                    Z.zzc_rzrq ,
                    Z.zqsz_rzrq ,
                    Z.zjye_rzrq ,
                    Z.zfz_rzrq ,
                    Z.yk_rzrq ,
                    Z.fdyk_rzrq ,
                    Z.zxjz_rzrq ,
                    Z.zxjz_zzl_rzrq ,
                    Z.zqsz_jrcp ,
                    Z.yk_jrcp ,
                    Z.fdyk_jrcp ,
                    Z.zzc_ggqq ,
                    Z.zjye_ggqq ,
                    Z.zqsz_ggqq ,
                    Z.yk_ggqq ,
                    Z.fdyk_ggqq ,
                    Z.zxjz_ggqq ,
                    Z.zxjz_zzl_ggqq ,
                    Z.rq FROM " ||
                l_tableName || " Z LEFT JOIN cust.VW_T_KHXX K ON Z.KHH = K.KHH";
                
      EXECUTE IMMEDIATE l_sqlBuf; 
    END IF;
  END;  
 END;
 /