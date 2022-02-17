!set plsqlUseSlash true
create or replace procedure CUST.P_IDS_MONTHLY_BILL_STANDARD(
  --�������
  I_KSRQ IN INT,    -- ��ʼ����
  I_JSRQ IN INT,    -- ��������
  I_BILLSTYPE IN STRING, --�˵����ͣ����˵���year|���˵���month/NULL
  I_KHH IN STRING

) 

IS
/******************************************************************
  *�ļ����ƣ�CUST.P_IDS_MONTHLY_BILL_STANDARD
  *��Ŀ���ƣ�IDS����
  *�ļ�˵�����˻�����-���˵�

  �����ˣ���ͳ
  ����˵�����˻�����-���˵�

  ����˵��

  �޸���        �汾��        �޸�����        ˵��
  ��ͳ            v1.0.0        2019/6/25       ����
  �����          v1.0.0        2019/07/18      ������̴��ڵ����⣬���������udf
*******************************************************************/
--I_conf configs;
--V_START STRING;
l_sj STRING;            --ʱ��
l_syrq INT;             --��ʼ���ڵ��ϸ�������
l_qmrq INT;             --��ĩ����
l_sqlBuf STRING;        --���������
l_tableName STRING;     --��ʱ����
tabCstr STRING;         --������ʱ��
l_hlcsHKD decimal(12,6);   --�۱һ���
l_hlcsUSD decimal(12,6);   --��Ԫ����
TYPE nest_table IS TABLE OF STRING;
l_tableArr nest_table DEFAULT NULL; --�ֶμ�
l_columns STRING; --�ֶ��ַ���
l_sql STRING; --ִ�е�sql���
l_initDate INT; --��ʼ�����ڣ�Ӧ��ȡ�����ÿ�
BEGIN
   /*
    * ����udf��������
    * �˺���Ҳ���Բ���decode()/case when���������������case when���
    * tranCurrency(double yj, int bz) -> f_ids_tran_currency(doule yk, int bz, decimal l_hlcsHKD, decimal l_hlcsUSD) return double
    * �˺������ڸ���splitChar��ֺ�ͳ����Ŀ
    * countSet(string string1, string splitChar) -> f_ids_set_count(string strings, string splitChar) return num
    * �˺���������������
    * sortProfitList(string profits, string field)-> f_ids_sort_profit_list(string profits, string field) return string
    */
    
    -- ��ʼ���������
    IF I_BILLSTYPE = 'year' THEN
      SELECT substr(I_KSRQ, 1, 4) INTO l_sj from  SYSTEM.DUAL;
    ELSE
      SELECT substr(I_KSRQ, 1, 6) INTO l_sj from  SYSTEM.DUAL;
    END IF;
    
    SELECT f_get_jyr_date(I_KSRQ,-1) INTO l_syrq from  SYSTEM.DUAL;
    SELECT f_get_jyr_date(I_JSRQ, 0) INTO l_qmrq from  SYSTEM.DUAL;
    
    SELECT f_get_hlcs('2', I_KSRQ) INTO l_hlcsHKD from  system.dual;
    SELECT f_get_hlcs('3', I_KSRQ) INTO l_hlcsUSD from  system.dual;
    
    l_initDate := 20190603;

    -- ���û������ϸ��ַ���
    set_env('character.literal.as.string',true);
            
    /*
     * ����hive���� 
     * ��ȡ�ս��׵����ֲ֡�����Լ����ֵĽ���
     * loadSourceDatas()����Դ����
     */
    BEGIN
        -- getDailyBillSql
        BEGIN
            l_tableName := F_IDS_GET_TABLENAME('bill_sparkRzd', I_KHH);
            l_sqlBuf := "select * from  cust.t_stat_zd_r where rq between " ||
                        I_KSRQ || " AND " || I_JSRQ || IF(I_KHH IS NULL, "" ," AND KHH = " || I_KHH);
                        
            --���ô�����ʱ����������ʱ����Σ������select��䣬��ʱ������
            F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
        END;
                  
        -- getJzjySecurityBalanceSql
        BEGIN
            l_tableName := F_IDS_GET_TABLENAME('bill_sparkJzjyZqye', I_KHH);
            l_sqlBuf := "select * from  cust.t_zqye_his where rq between " || 
                        I_KSRQ || " and " || I_JSRQ || IF(I_KHH IS NULL, "" ," AND KHH = " || I_KHH);
            
            --���ô�����ʱ����������ʱ����Σ������select��䣬��ʱ������
            F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
        END;
        
         -- getXYSecurityBalanceSql
        BEGIN
            l_tableName := F_IDS_GET_TABLENAME('bill_sparkXYzqye', I_KHH);
            l_sqlBuf := "select * from  cust.t_xy_zqye_his where rq between " || 
                        I_KSRQ || " and " || I_JSRQ || IF(I_KHH IS NULL, "" ," AND KHH = " || I_KHH);
            
            --���ô�����ʱ����������ʱ����Σ������select��䣬��ʱ������
            F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
        END;
        
        -- getFPProductShare
        BEGIN
            l_tableName := F_IDS_GET_TABLENAME('bill_sparkFPcpfe', I_KHH);
            l_sqlBuf := "select * from  cust.t_fp_cpfe_his where rq between " || 
                        I_KSRQ || " and "  || I_JSRQ || IF(I_KHH IS NULL, "" ," AND KHH = " || I_KHH);
            
            --���ô�����ʱ����������ʱ����Σ������select��䣬��ʱ������
            F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
        END;
        
        -- getOptionContractBalanceSql
        BEGIN
            l_tableName := F_IDS_GET_TABLENAME('bill_sparkSoHycc', I_KHH);
            l_sqlBuf := "select * from  cust.t_so_zqye_his where rq between " || 
                        I_KSRQ || " and " || I_JSRQ || IF(I_KHH IS NULL, "" ," AND KHH = " || I_KHH);
            
            --���ô�����ʱ����������ʱ����Σ������select��䣬��ʱ������
            F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
        END;
        
        -- getJzjyInvestProfit  
        BEGIN
            l_tableName := F_IDS_GET_TABLENAME('bill_sparkJzjyTzsy', I_KHH);
            l_sqlBuf := "select * from  cust.t_tzsy where qcrq between " || 
                        I_KSRQ || " and " || I_JSRQ || IF(I_KHH IS NULL, "" ," AND KHH = " || I_KHH);
            
            --���ô�����ʱ����������ʱ����Σ������select��䣬��ʱ������
            F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
        END;
        
         -- getXYInvestProfit
        BEGIN
            l_tableName := F_IDS_GET_TABLENAME('bill_sparkXyTzsy', I_KHH);
            l_sqlBuf := "select * from  cust.t_xy_tzsy where qcrq between " || 
                        I_KSRQ || " and " || I_JSRQ || IF(I_KHH IS NULL, "" ," AND KHH = " || I_KHH);
            
            --���ô�����ʱ����������ʱ����Σ������select��䣬��ʱ������
            F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
        END;
        
        -- getFPInvestProfit 
        BEGIN
            l_tableName := F_IDS_GET_TABLENAME('bill_sparkFpTzsy', I_KHH);
            l_sqlBuf := "select * from  cust.t_fp_tzsy where qcrq between " || 
                        I_KSRQ || " and " || I_JSRQ || IF(I_KHH IS NULL, "" ," AND KHH = " || I_KHH);
            
            --���ô�����ʱ����������ʱ����Σ������select��䣬��ʱ������
            F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
        END;
        
        -- getOptionInvestProfit
        BEGIN
            l_tableName := F_IDS_GET_TABLENAME('bill_sparkSoTzsy', I_KHH);
            l_sqlBuf := "select * from  cust.t_so_tzsy where qcrq between " || 
                        I_KSRQ || " and " || I_JSRQ || IF(I_KHH IS NULL, "" ," AND KHH = " || I_KHH);
            
            --���ô�����ʱ����������ʱ����Σ������select��䣬��ʱ������
            F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
        END;        
        
        -- getJzjyDeliveryOrderGTSql 
        BEGIN
            l_tableName := F_IDS_GET_TABLENAME('bill_sparkJzjyJgls', I_KHH);
            l_sqlBuf := "select * from  cust.t_jgmxls_his where cjrq between " || 
                        I_KSRQ || " and " || I_JSRQ || IF(I_KHH IS NULL, "" ," AND KHH = " || I_KHH);
            
            --���ô�����ʱ����������ʱ����Σ������select��䣬��ʱ������
            F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
        END;
        
        -- getXYDeliveryOrderSql
        BEGIN
            l_tableName := F_IDS_GET_TABLENAME('bill_sparkXyJgls', I_KHH);
            l_sqlBuf := "select * from  cust.t_xy_jgmxls_his where cjrq between " || 
                        I_KSRQ || " and " || I_JSRQ || IF(I_KHH IS NULL, "" ," AND KHH = " || I_KHH);
            
            --���ô�����ʱ����������ʱ����Σ������select��䣬��ʱ������
            F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName); 
        END;
        
        BEGIN
            l_tableName := F_IDS_GET_TABLENAME('bill_sparkZqlb', I_KHH);
            l_sqlBuf := "select * from  DSC_CFG.VW_T_ZQLB_IDS"
            
            --���ô�����ʱ����������ʱ����Σ������select��䣬��ʱ������
            F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
        END;
        
        BEGIN
            l_tableName := F_IDS_GET_TABLENAME('bill_sparkZcpz', I_KHH);
            l_sqlBuf := "select zqdm,jys,zqlb,zcpzflbm,zcpzflmc from  " ||
                    "(select zqdm,jys,zqlb,zcpzflbm,zcpzflmc,row_number() over(partition by jys,zqdm order by whrq desc) rn " ||
                    "   from  info.TZQDM_ZCPZWH) a where rn=1";
            
            F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
        END;
        
        BEGIN
            l_tableName := F_IDS_GET_TABLENAME('bill_sparkGsgk', I_KHH);
            l_sqlBuf := "select jys,zqdm,sshy from  (select *,row_number() over(partition by jys,zqdm order by id desc) rn from  "  ||
                        "info.tgp_gsgk where length(sshy)>0 and sshy!='null' and sshy!='��') where rn=1 ";
            
            F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
        END;
        
        -- �ֲ��Լ���ֺ���  tranCurrency ���UDF�����Ѿ�����case when�滻
        BEGIN
            l_tableName := F_IDS_GET_TABLENAME('bill_sparkTzfb', I_KHH);
            l_sqlBuf := "select " ||
                        " khh,"   ||
                        " z.rq,"  ||
                        " z.jys," ||
                        " z.zqdm," ||
                        " nvl(z.zqmc, z.zqdm) as zqmc," ||
                        " dryk," ||
                        " case when z.lb='fp' then '���' when z.lb='so' then '��Ȩ' else NVL(a.zqpzmc,'��Ʊ') END as zqpz," ||
                        " case when z.lb in ('fp','so') then 'Ȩ����' else NVL(b.zcpzflmc, '����') END as zcpz," ||
                        " regexp_replace(c.sshy,'[,; ]','') as sshy," ||
                        " z.lb," ||
                        " z.fdyk," ||
                        " z.zxsz," ||
                        " z.cccb" ||
                        " from  " ||
                        "(select khh,z.rq,z.jys,z.zqdm,d.zqmc,CAST(CASE WHEN z.bz = 1 THEN dryk WHEN z.bz=2 THEN dryk * " || l_hlcsHKD || " WHEN z.bz = 3 THEN dryk * " || l_hlcsUSD || " END AS double) as dryk,z.zqlb,'jzjy' as lb,CAST(CASE WHEN z.bz = 1 THEN zxsz-tbcccb WHEN z.bz=2 THEN (zxsz-tbcccb) * " || l_hlcsHKD || " WHEN z.bz = 3 THEN (zxsz-tbcccb) * " || l_hlcsUSD || " END AS double) as fdyk,CAST(CASE WHEN z.bz = 1 THEN zxsz WHEN z.bz=2 THEN zxsz * " || l_hlcsHKD || " WHEN z.bz = 3 THEN zxsz * " || l_hlcsUSD || " END AS double) as zxsz,CAST(CASE WHEN z.bz = 1 THEN cccb WHEN z.bz=2 THEN cccb * " || l_hlcsHKD || " WHEN z.bz = 3 THEN cccb * " || l_hlcsUSD || " END AS double) as cccb from  " ||
                        F_IDS_GET_TABLENAME('bill_sparkJzjyZqye', I_KHH) ||" z left join cust.t_zqdm d on (z.jys=d.jys and z.zqdm=d.zqdm)" ||
                        " union ALL " ||
                        " select khh,z.qcrq as rq,z.jys,z.zqdm,d.zqmc,CAST(CASE WHEN z.bz = 1 THEN dryk WHEN z.bz=2 THEN dryk * " || l_hlcsHKD || " WHEN z.bz = 3 THEN dryk * " || l_hlcsUSD || " END AS double) as dryk,z.zqlb,'jzjy' as lb,0.0 as fdyk,0.0 as zxsz,0.0 as cccb from  " ||
                        F_IDS_GET_TABLENAME('bill_sparkJzjyTzsy', I_KHH) ||" z left join cust.t_zqdm d on (z.jys=d.jys and z.zqdm=d.zqdm)" ||
                        " union all" ||
                        " select khh,z.rq,z.jrjgdm as jys,z.cpdm as zqdm,d.cpjc as zqmc,CAST(CASE WHEN z.bz = 1 THEN dryk WHEN z.bz=2 THEN dryk * " || l_hlcsHKD || " WHEN z.bz = 3 THEN dryk * " || l_hlcsUSD || " END AS double) as dryk,cast('' as string) as zqlb,'fp' as lb,CAST(CASE WHEN z.bz = 1 THEN zxsz-tbcccb WHEN z.bz=2 THEN (zxsz-tbcccb) * " || l_hlcsHKD || " WHEN z.bz = 3 THEN (zxsz-tbcccb) * " || l_hlcsUSD || " END AS double) as fdyk,CAST(CASE WHEN z.bz = 1 THEN zxsz WHEN z.bz=2 THEN zxsz * " || l_hlcsHKD || " WHEN z.bz = 3 THEN zxsz * " || l_hlcsUSD || " END AS double) as zxsz,CAST(CASE WHEN z.bz = 1 THEN cccb WHEN z.bz=2 THEN cccb * " || l_hlcsHKD || " WHEN z.bz = 3 THEN cccb * " || l_hlcsUSD || " END AS double) as cccb from  " ||
                        F_IDS_GET_TABLENAME('bill_sparkFPcpfe', I_KHH) ||" z left join cust.t_jrcpdm d on (z.jrjgdm=d.jrjgdm and z.cpdm=d.cpdm and z.app_id=d.app_id)" ||
                        " union all" ||
                        " select khh,z.qcrq as rq,z.jrjgdm as jys,z.cpdm as zqdm,d.cpjc as zqmc,CAST(CASE WHEN z.bz = 1 THEN dryk WHEN z.bz=2 THEN dryk * " || l_hlcsHKD || " WHEN z.bz = 3 THEN dryk * " || l_hlcsUSD || " END AS double) as dryk,cast('' AS STRING) as zqlb,'fp' as lb,0.0 as fdyk,0.0 as zxsz,0.0 as cccb from  " ||
                        F_IDS_GET_TABLENAME('bill_sparkFpTzsy', I_KHH) ||" z left join cust.t_jrcpdm d on (z.jrjgdm=d.jrjgdm and z.cpdm=d.cpdm and z.app_id=d.app_id)" ||
                        " union all" ||
                        " select khh,z.rq,z.jys,z.zqdm,d.zqmc,CAST(CASE WHEN z.bz = 1 THEN dryk WHEN z.bz=2 THEN dryk * " || l_hlcsHKD || " WHEN z.bz = 3 THEN dryk * " || l_hlcsUSD || " END AS double) as dryk,z.zqlb,'rzrq' as lb,CAST(CASE WHEN z.bz = 1 THEN zxsz-tbcccb WHEN z.bz=2 THEN (zxsz-tbcccb) * " || l_hlcsHKD || " WHEN z.bz = 3 THEN (zxsz-tbcccb) * " || l_hlcsUSD || " END AS double) as fdyk,CAST(CASE WHEN z.bz = 1 THEN zxsz WHEN z.bz=2 THEN zxsz * " || l_hlcsHKD || " WHEN z.bz = 3 THEN zxsz * " || l_hlcsUSD || " END AS double) as zxsz,CAST(CASE WHEN z.bz = 1 THEN cccb WHEN z.bz=2 THEN cccb * " || l_hlcsHKD || " WHEN z.bz = 3 THEN cccb * " || l_hlcsUSD || " END AS double) as cccb from  " ||
                        F_IDS_GET_TABLENAME('bill_sparkXYzqye', I_KHH) ||" z left join cust.t_zqdm d on (z.jys=d.jys and z.zqdm=d.zqdm)" ||
                        " union ALL " ||
                        " select khh,z.qcrq as rq,z.jys,z.zqdm,d.zqmc,CAST(CASE WHEN z.bz = 1 THEN dryk WHEN z.bz=2 THEN dryk * " || l_hlcsHKD || " WHEN z.bz = 3 THEN dryk * " || l_hlcsUSD || " END AS double) as dryk,z.zqlb,'rzrq' as lb,0.0 as fdyk,0.0 as zxsz,0.0 as cccb from  " ||
                        F_IDS_GET_TABLENAME('bill_sparkXyTzsy', I_KHH) ||" z left join cust.t_zqdm d on (z.jys=d.jys and z.zqdm=d.zqdm)" ||
                        " union ALL " ||
                        " select khh,z.rq,z.jys,z.hydm as zqdm,nvl(d.hymc,z.hymc) as zqdmc,CAST(CASE WHEN z.bz = 1 THEN dryk WHEN z.bz=2 THEN dryk * " || l_hlcsHKD || " WHEN z.bz = 3 THEN dryk * " || l_hlcsUSD || " END AS double) as dryk,cast(''as string) as zqlb,'so' as lb,CAST(CASE WHEN z.bz = 1 THEN zxsz-tbcccb WHEN z.bz=2 THEN (zxsz-tbcccb) * " || l_hlcsHKD || " WHEN z.bz = 3 THEN (zxsz-tbcccb) * " || l_hlcsUSD || " END AS double) as fdyk,CAST(CASE WHEN z.bz = 1 THEN zxsz WHEN z.bz=2 THEN zxsz * " || l_hlcsHKD || " WHEN z.bz = 3 THEN zxsz * " || l_hlcsUSD || " END AS double) as zxsz,CAST(CASE WHEN z.bz = 1 THEN cccb WHEN z.bz=2 THEN cccb * " || l_hlcsHKD || " WHEN z.bz = 3 THEN cccb * " || l_hlcsUSD || " END AS double) as cccb from  " ||
                        F_IDS_GET_TABLENAME('bill_sparkSoHycc', I_KHH) ||" z left join cust.t_so_hydm d on (z.jys=d.jys and z.hydm=d.hydm)" ||
                        " union ALL " ||
                        " select khh,z.qcrq as rq,z.jys,z.hydm as zqdm,nvl(d.hymc,z.hydm) as zqdmc,CAST(CASE WHEN z.bz = 1 THEN dryk WHEN z.bz=2 THEN dryk * " || l_hlcsHKD || " WHEN z.bz = 3 THEN dryk * " || l_hlcsUSD || " END AS double) as dryk,cast(''as string) as zqlb,'so' as lb,0.0 as fdyk,0.0 as zxsz,0.0 as cccb from  " ||
                        F_IDS_GET_TABLENAME('bill_sparkSoTzsy', I_KHH) || " z left join cust.t_so_hydm d on (z.jys=d.jys and z.hydm=d.hydm)) z " ||
                        " left JOIN " ||
                        F_IDS_GET_TABLENAME('bill_sparkZqlb', I_KHH) ||" a on (z.zqlb=a.zqlb and z.jys=a.jys)" ||
                        " left JOIN " ||
                        F_IDS_GET_TABLENAME('bill_sparkZcpz', I_KHH) ||" b on (z.zqdm=b.zqdm and z.jys=b.jys)" ||
                        " left JOIN " ||
                        F_IDS_GET_TABLENAME('bill_sparkGsgk', I_KHH) ||" c on (z.jys=c.jys and z.zqdm=c.zqdm)";
        
            F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
        END;
    END;
    
    /*
     * assertAnalysis() �ʲ���������
     */
    BEGIN
	    l_tableName := F_IDS_GET_TABLENAME('bill_sparkZcfx', I_KHH);
        l_sqlBuf := " SELECT 
                 f_ids_month_analyze(row, " ||  I_KSRQ || ", " || I_JSRQ || ", " ||  l_initDate || " )
            FROM
                (SELECT khh,
                    GroupRow(
                        khh,
                        rq,
                        zzc,
                        zzc_jzjy,
                        zzc_rzrq,
                        zzc_ggqq,
                        zjye,
                        zjye_jzjy,
                        zjye_rzrq,
                        zjye_ggqq,
                        zcjlr,
                        crje,
                        qcje,
                        zrzqsz,
                        zczqsz,
                        yk,
                        yk_jzjy,
                        yk_rzrq,
                        yk_jrcp,
                        yk_ggqq,
                        zqsz,
                        zqsz_jzjy,
                        zqsz_rzrq,
                        zqsz_jrcp,
                        zqsz_ggqq,
                        zfz,
                        zfz_rzrq,
                        zxjz,
                        zxjz_jzjy,
                        zxjz_rzrq,
                        zxjz_ggqq) AS ROW FROM " ||
                        F_IDS_GET_TABLENAME('bill_sparkRzd', I_KHH) || " GROUP BY khh) a";

        F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
	END;
    
    /*
     * securityTrade() ��Ʊ���׷�������
     */
    BEGIN
        BEGIN
            l_tableName := F_IDS_GET_TABLENAME('bill_sparkJgls', I_KHH);
            l_sqlBuf :=  "select z.khh,z.cjrq,z.jys,z.zqdm,z.zqlb,z.jylb,z.ysje,z.yssl,z.lb,a.zqpzmc,regexp_replace(b.sshy,'[,; ]','') as sshy " ||
                    " from  " ||
                    "(select khh,cjrq,jys,zqdm,zqlb,jylb,ysje,yssl,'jzjy' as lb from  " ||
                    F_IDS_GET_TABLENAME('bill_sparkJzjyJgls', I_KHH) ||
                    " union all " ||
                    " select khh,cjrq,jys,zqdm,zqlb,jylb,ysje,yssl,'rzrq' as lb from  " ||
                    F_IDS_GET_TABLENAME('bill_sparkXyJgls', I_KHH) || " ) z " ||
                    " left JOIN " ||
                    F_IDS_GET_TABLENAME('bill_sparkZqlb', I_KHH) ||" a on (z.zqlb=a.zqlb and z.jys=a.jys) " ||
                    " left JOIN " ||
                    F_IDS_GET_TABLENAME('bill_sparkGsgk', I_KHH) ||" b on (z.jys=b.jys and z.zqdm=b.zqdm)";
                    
            F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
        END;
        
        BEGIN
            l_tableName := F_IDS_GET_TABLENAME('bill_sparkJyfx1', I_KHH);
            l_sqlBuf :=  "select " ||
                        " khh," ||
                        " jys," ||
                        " zqdm," ||
                        " lb," ||
                        " zqpzmc," ||
                        " sshy," ||
                        " sum(case when jylb='1' then 1 else 0 END) as mrcs," ||
                        " sum(case when jylb='2' then 1 else 0 END) as mccs," ||
                        " sum(case when jylb='1' then abs(ysje) else 0 END) as mrje," ||
                        " sum(case when jylb='2' then abs(ysje) else 0 END) as mcje," ||
                        " sum(case when jylb='1' then abs(yssl) else 0 END) as mrsl," ||
                        " sum(case when jylb='2' then abs(yssl) else 0 END) as mcsl," ||
                        " count(1) as czcs," ||
                        " SUM(ABS(ysje)) as jyl, " ||
                        " cjrq from  " ||
                        F_IDS_GET_TABLENAME('bill_sparkJgls', I_KHH) ||" group by khh,jys,zqdm,lb,cjrq,zqpzmc,sshy";
            
            F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
        END;
        
        BEGIN
            l_tableName := F_IDS_GET_TABLENAME('bill_sparkGpjyfx', I_KHH);
            l_sqlBuf := " select " ||
                        " khh," ||
                        " sum(czcs) as czcs," ||
                        " sum(case when mrsl>0 and mcsl>0 then 1 else 0 END) as ztcs," ||
                        " sum(case when mrsl>0 and mcsl>0 then if((mcje/mcsl)>(mrje/mrsl),1,0) else 0 END) / " ||
                        "    sum(case when mrsl>0 and mcsl>0 then 1 else 0 END) as ztcgl," ||
                        " sum(case when zqpzmc='��Ʊ' and mrsl>0 and mcsl>0 then 1 else 0 END) as ztcs_gp," ||
                        " sum(case when zqpzmc='��Ʊ' and mrsl>0 and mcsl>0 then if((mcje/mcsl)>(mrje/mrsl),1,0) else 0 END) / " ||
                        "    sum(case when zqpzmc='��Ʊ' and mrsl>0 and mcsl>0 then 1 else 0 END) as ztcgl_gp," ||
                        " f_ids_set_count(concat_ws(',',collect_set(case when zqpzmc='��Ʊ' and mrsl>0 and mcsl>0 then concat(jys,zqdm,lb) else '' END)), ',') as ztsl_gp," ||
                        " sum(case when zqpzmc='��Ʊ' then mrje else 0 END) as cjje_gp_mr," ||
                        " sum(case when zqpzmc='��Ʊ' then mcje else 0 END) as cjje_gp_mc," ||
                        " sum(case when zqpzmc='��Ʊ' then mrcs else 0 END) as jybs_gp_mr," ||
                        " sum(case when zqpzmc='��Ʊ' then mccs else 0 END) as jybs_gp_mc from  " ||
                        F_IDS_GET_TABLENAME('bill_sparkJyfx1', I_KHH) ||" group by khh";
            
            F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
        END;
        
        BEGIN
            l_tableName := F_IDS_GET_TABLENAME('bill_sparkJyHyph', I_KHH);
            l_sqlBuf :=  "select " ||
                        " khh," ||
                        " sshy," ||
                        " SUM(jyl) as jyl from  " ||
                        F_IDS_GET_TABLENAME('bill_sparkJyfx1', I_KHH) ||" where sshy is not null group by khh,sshy";
            
            F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
        END;
    END;
    
    /*
     * positionAnalyze() �ֲַ���
     */
    BEGIN
        
        -- ӯ�����������б�
        BEGIN
            l_tableName := F_IDS_GET_TABLENAME('bill_sparkYklist1', I_KHH);
            l_sqlBuf := " select " ||
                        " khh," ||
                        " concat_ws('&',collect_set(lb)) as lb," ||
                        " jys," ||
                        " zqdm," ||
                        " zqmc," ||
                        " sum(dryk) as ljyk from  " ||
                        F_IDS_GET_TABLENAME('bill_sparkTzfb', I_KHH) ||" group by khh,jys,zqdm,zqmc";
            
            F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
        END;
        
        BEGIN
            
            /*
             *  SparkUdf��sortProfitList ��Ϊ HiveUdf ��f_ids_sort_profit_list
             */ 
            
            l_tableName := F_IDS_GET_TABLENAME('bill_sparkYklist', I_KHH);
            l_sqlBuf := " select " ||
                        "   khh, " ||
                        "   f_ids_sort_profit_list(concat_ws('',collect_set(yl_list)),'ljyk') as yl_list," ||
                        "   f_ids_sort_profit_list(concat_ws('',collect_set(ks_list)),'ljyk') as ks_list," ||
                        "   case when sum(ylgs)=0 and sum(ksgs)=0 then 0 else sum(ylgs)/(sum(ylgs)+sum(ksgs)) END as xgcgl" ||
                        " from  " ||
                        "(select " ||
                        "   khh," ||
                        "   concat_ws(';',collect_set(concat('gtlb\:',lb,','," ||
                        "                                    'jys\:',jys,','," ||
                        "                                    'zqdm\:',zqdm,','," ||
                        "                                    'zqmc\:',zqmc,','," ||
                        "                                    'ljyk\:',ljyk) )) as yl_list," ||
                        "   cast('' AS STRING) as ks_list," ||
                        "   count(1) as ylgs," ||
                        "   0 as ksgs from  " ||
                        F_IDS_GET_TABLENAME('bill_sparkYklist1', I_KHH) ||" where ljyk > 0 group by khh" ||
                        " union all" ||
                        " select " ||
                        "   khh," ||
                        "   cast('' AS STRING) as yl_list," ||
                        "   concat_ws(';',collect_set(concat('gtlb\:',lb,',',  " ||
                        "                                    'jys\:',jys,',',  " ||
                        "                                    'zqdm\:',zqdm,','," ||
                        "                                    'zqmc\:',zqmc,','," ||
                        "                                    'ljyk\:',ljyk) )) as ks_list," ||
                        "   0 as ylgs," ||
                        "   count(1) as ksgs from  " ||
                        F_IDS_GET_TABLENAME('bill_sparkYklist1', I_KHH) ||" where ljyk<0 group by khh) a group by khh";
        
            F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
        END;
        
        -- ֤ȯƷ���б�
        BEGIN
            l_tableName := F_IDS_GET_TABLENAME('bill_sparkYkZqpzList', I_KHH);
            l_sqlBuf := "select " ||
                    "   khh," ||
                    "   concat('{',concat_ws(',',collect_set(concat_ws('\:', concat('\"',zqpz,'\"'), concat('\"',cast(ljyl as string),'\"')) )),'}') as yl_zqpz_list," ||
                    "   concat('{',concat_ws(',',collect_set(concat_ws('\:', concat('\"',zqpz,'\"'), concat('\"',cast(ljks as string),'\"')) )),'}') as ks_zqpz_list" ||
                    " from  " ||
                    "(select" ||
                    "   khh," ||
                    "   zqpz," ||
                    "   sum(case when dryk>0 then dryk else 0 END) as ljyl," ||
                    "   sum(case when dryk<0 then dryk else 0 END) as ljks " ||
                    " from  " ||
                    "(select" ||
                    "   khh," ||
                    "   zqpz," ||
                    "   lb," ||
                    "   jys," ||
                    "   zqdm," ||
                    "   sum(dryk) as dryk from  " ||
                    F_IDS_GET_TABLENAME('bill_sparkTzfb', I_KHH) || " group by khh,zqpz,lb,jys,zqdm) a group by khh,zqpz)a group by khh";
            
            F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
        END;
        
        -- ��Ʊ�ֲ�ͳ��
        BEGIN
            l_tableName := F_IDS_GET_TABLENAME('bill_sparkGpYk', I_KHH);
            l_sqlBuf := "select " ||
                        "  khh," ||
                        "  if(cggs_gp=0, 0, cgcgs_gp/cggs_gp) as cgcgl_gp," ||
                        "  cggs_gp," ||
                        "  zsz_gp," ||
                        "  yk_gp " ||
                        " from  " ||
                        "(select " ||
                        "  khh," ||
                        "  sum(case when ljyk>0 then 1 else 0 END) as cgcgs_gp," ||
                        "  count(DISTINCT concat(lb,jys,zqdm)) as cggs_gp," ||
                        "  sum(ljyk) as yk_gp," ||
                        "  sum(zxsz) as zsz_gp" ||
                        " from " ||
                        "(select " ||
                        "  khh," ||
                        "  concat_ws('&',collect_set(lb)) as lb," ||
                        "  jys," ||
                        "  zqdm," ||
                        "  sum(dryk) AS ljyk," ||
                        "  sum(zxsz) AS zxsz from  " ||
                        F_IDS_GET_TABLENAME('bill_sparkTzfb', I_KHH) ||" where zqpz='��Ʊ' group by khh,jys,zqdm)a group by khh)a ";
            
            F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
        END;
        
        -- �ְֲ��
        BEGIN
            l_tableName := F_IDS_GET_TABLENAME('bill_sparkZqyeHyph', I_KHH);
            l_sqlBuf := "select " ||
                        " khh," ||
                        " sshy," ||
                        " sum(cccb) as cccb from  " ||
                        F_IDS_GET_TABLENAME('bill_sparkTzfb', I_KHH) ||" where sshy is not null group by khh,sshy";
            
            F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
        END;
    END;
    
    /*
     * industryPreference() �ֲ�ƫ�ý��׷���,�ֲַ������к�ִ��,��ʱ������
     */
    BEGIN
        l_tableName := F_IDS_GET_TABLENAME('bill_sparkHyph', I_KHH);
        l_sqlBuf := "select " ||
                "   khh," ||
                "   sshy" ||
                " from   " ||
                "(select " ||
                "   khh, " ||
                "   sshy," ||
                "   row_number() over(partition by khh order by zbz desc nulls last) rn" ||
                " from    " ||
                "(select " ||
                "   khh, " ||
                "   sshy," ||
                "   sum(zbz) as zbz" ||
                " from    " ||
                "(select " ||
                "   khh, " ||
                "   sshy," ||
                "   cccb as zbz" ||
                " from  "|| 
                F_IDS_GET_TABLENAME('bill_sparkZqyeHyph', I_KHH) ||
                " union all " ||
                " select" ||
                "   khh," ||
                "   sshy," ||
                "   jyl as zbz from  " ||
                F_IDS_GET_TABLENAME('bill_sparkJyHyph', I_KHH) ||" )a group by khh,sshy)a)a where rn=1";
        
        F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
    END;
    
    /*
     * �������˵�������Ͷ�ʷ�ʽ����
     */
    BEGIN
        IF I_BILLSTYPE = 'year' THEN
            /*
             * investStyle() Ͷ�ʷ�ʽ����
             */
            BEGIN
                BEGIN
                    l_tableName := F_IDS_GET_TABLENAME('bill_sparkTzfs1', I_KHH);
                    l_sqlBuf := " select" ||
                                "   z.khh," ||
                                "   z.zzc," ||
                                "   z.rq," ||
                                "   t.lb," ||
                                "   t.jys," ||
                                "   t.zqdm," ||
                                "   if(z.zzc=0,0,nvl(t.zxsz,0.0)/z.zzc) as zb" ||
                                " from  " ||
                                F_IDS_GET_TABLENAME('bill_sparkRzd', I_KHH) || 
                                " z  left join " || 
                                F_IDS_GET_TABLENAME('bill_sparkTzfb', I_KHH) ||" t on (z.khh=t.khh and z.rq=t.rq)";
                    
                    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
                END;
                
                BEGIN
                    l_tableName := F_IDS_GET_TABLENAME('bill_sparkTzfs', I_KHH);
                    l_sqlBuf := " select KHH, " ||
                                " SUM(case when cxtz >= 150 then 1 else 0 end) AS cxtzgs, " ||
                                " SUM(case when zxtz >= 100 then 1 else 0 end) AS zxtzgs "  ||
                                " from  (SELECT KHH, " ||
                                " LB, " ||
                                " JYS," ||
                                " ZQDM," ||
                                " SUM(case when zb>=0.5 and syrzb>=0.5 and hyrzb>=0.5 then 1 else 0 end) as cxtz, " ||
                                " SUM(case when zb>=0.3 and zb<0.5 and syrzb>=0.3 and syrzb<0.5 and hyrzb>=0.3 and hyrzb<0.5 then 1 else 0 end) as zxtz " ||
                                " from  (SELECT " ||
                                " KHH, "||
                                " ZZC, " ||
                                " RQ, " ||
                                " LB, " ||
                                " JYS, " ||
                                " ZQDM, " ||
                                " ZB, " ||
                                " LAG(zb,1,zb) over(partition by khh,lb,jys,zqdm order by rq) as syrzb, " ||
                                " LEAD(zb,1,zb) over(partition by khh,lb,jys,zqdm order by rq) as hyrzb " ||
                                " from  " ||
                                F_IDS_GET_TABLENAME('bill_sparkTzfs1', I_KHH) || ") sparkTzfs2 GROUP BY KHH, LB, JYS, ZQDM) sparkTzfs3 GROUP BY KHH";
                    
                    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
                END;
            END;
            
            /*
             * investFootprint() Ͷ���㼣
             */
            BEGIN
                -- ���ºϲ��������ۼ�
                BEGIN
                    l_tableName := F_IDS_GET_TABLENAME('bill_sparkTzzj2', I_KHH);
                    l_sqlBuf := "select" ||
                                "   khh," ||
                                "   yf," ||
                                "   lb," ||
                                "   jys," ||
                                "   zqdm," ||
                                "   zqmc," ||
                                "   sum(byyk) over(partition by khh,lb,jys,zqdm,zqmc order by yf) ljyk" ||
                                " from  (select " ||
                                "   khh," ||
                                "   substr(cast(rq as string),1,6) as yf," ||
                                "   concat_ws('&',collect_set(lb)) as lb," ||
                                "   jys," ||
                                "   zqdm," ||
                                "   zqmc," ||
                                "   sum(dryk) as byyk" ||
                                " from  " || F_IDS_GET_TABLENAME('bill_sparkTzfb', I_KHH) ||
                                " group by khh,jys,zqdm,zqmc,substr(cast(rq as string),1,6)) sparkTzzj1";
                    
                    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);                
                END;
                
                -- ÿ����������
                BEGIN
                    l_tableName := F_IDS_GET_TABLENAME('bill_sparkTzzj3', I_KHH);
                    l_sqlBuf := "select"  ||
                                "   khh," ||
                                "   yf, " ||
                                "   lb, " ||
                                "   jys," ||
                                "   zqdm," ||
                                "   zqmc," ||
                                "   row_number() over(partition by khh,yf order by ljyk desc nulls last) as drn," ||
                                "   row_number() over(partition by khh,yf order by ljyk nulls last) as rn," ||
                                "   sum(ljyk) over(partition by khh order by yf) as yk," ||
                                "   ljyk " ||
                                " from  " || F_IDS_GET_TABLENAME('bill_sparkTzzj2', I_KHH);
                   
                    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName); 
                END;
                
                -- ���ݻ���
                BEGIN
                    l_tableName := F_IDS_GET_TABLENAME('bill_sparkTzzj', I_KHH);
                    l_sqlBuf := "select " ||
                                "   khh," ||
                                "   f_ids_sort_profit_list(concat_ws(';',collect_set(yk_list)),'yf') as ykzj" ||
                                " from " ||
                                "(select " ||
                                "   khh," ||
                                "   concat('yf\:',yf,',','gtlb\:',lb,',','jys\:',jys,',','zqdm\:',zqdm,',','zqmc\:',zqmc,',','ljyk\:',ljyk) as yk_list" ||
                                " from  " ||
                                F_IDS_GET_TABLENAME('bill_sparkTzzj3', I_KHH) || " where yk>=0 and drn=1 " ||
                                " union all " ||
                                " select" ||
                                "   khh," ||
                                "   concat('yf\:',yf,',','gtlb\:',lb,',','jys\:',jys,',','zqdm\:',zqdm,',','zqmc\:',zqmc,',','ljyk\:',ljyk) as yk_list" ||
                                " from  " ||
                                F_IDS_GET_TABLENAME('bill_sparkTzzj3', I_KHH) ||" where yk<0 and rn=1)a group by khh";
                  F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName); 
                END;
            END;
        END IF;
    END;
    
    /*
     * mergeAndGetResultBill() �ϲ��������˵�
     */
    BEGIN   
        BEGIN
            -- �������ս����
            l_tableName := F_IDS_GET_TABLENAME('bill_sparkResult0', I_KHH);
            --�ʲ�����
            l_sqlBuf := "select" ||
                      "   khh," ||
                      "   qmzzc," ||
                      "   qczzc," ||
                      "   qmzjye," ||
                      "   qmzqsz," ||
                      "   qmzfz," ||
                      "   yk," ||
                      "   ykl," ||
                      "   nhsyl," ||
                      "   crje," ||
                      "   qcje," ||
                      "   zrzqsz," ||
                      "   zczqsz," ||
                      "   zcjlr," ||
                      "   bdl," ||
                      "   zdhcl," ||
                      "   pjzzc," ||
                      "   pjsz," ||
                      "   byts," ||
                      "   0 as zsz_gp," ||
                      "   0 as yk_gp," ||
                      "   0 as cggs_gp," ||
                      "   0 as cgcgl_gp," ||
                      "   0 as cjje_gp_mr," ||
                      "   0 as cjje_gp_mc," ||
                      "   0 as jybs_gp_mr," ||
                      "   0 as jybs_gp_mc," ||
                      "   0 as ztcs_gp," ||
                      "   0 as ztsl_gp," ||
                      "   0 as ztcgl_gp," ||
                      "   cast('' as string) as yl_list," ||
                      "   cast('' as string) as ks_list," ||
                      "   cast('' as string) as yl_zqpz_list," ||
                      "   cast('' as string) as ks_zqpz_list," ||
                      "   0 as ztcgl," ||
                      "   0 as xgcgl," ||
                      "   CAST('' AS STRING) as sshy," ||
                      "   0 as ztcs," ||
                      "   0 as czcs," ||
                      "   qmzzc_jzjy," ||
                      "   qczzc_jzjy," ||
                      "   qmzqsz_jzjy," ||
                      "   qmzjye_jzjy," ||
                      "   yk_jzjy," ||
                      "   qmzxjz_jzjy," ||
                      "   zxjz_zzl_jzjy," ||
                      "   qmzzc_rzrq," ||
                      "   qczzc_rzrq," ||
                      "   qmzqsz_rzrq," ||
                      "   qmzjye_rzrq," ||
                      "   qmzfz_rzrq," ||
                      "   yk_rzrq," ||
                      "   qmzxjz_rzrq," ||
                      "   zxjz_zzl_rzrq," ||
                      "   qmzqsz_jrcp," ||
                      "   yk_jrcp," ||
                      "   qmzzc_ggqq," ||
                      "   qmzjye_ggqq," ||
                      "   qmzqsz_ggqq," ||
                      "   yk_ggqq," ||
                      "   qmzxjz_ggqq," ||
                      "   zxjz_zzl_ggqq from  " ||
                      F_IDS_GET_TABLENAME('bill_sparkZcfx', I_KHH);                    -- �����ΪassertAnalysis�������
            
            F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
        END;
        
                ---------------------------�������ս������ȡ���ֶΣ������������д�룬�������union all----------------------------------------------
        l_tableArr := get_columns(l_tableName);
        l_columns := ' ( ';
        FOR indx IN l_tableArr.first() .. l_tableArr.last() LOOP
            IF indx = l_tableArr.last() THEN
                l_columns := l_columns || l_tableArr(indx) || ') ';
            ELSE
                l_columns := l_columns || l_tableArr(indx) || ', ';
            END IF;
        END LOOP;  
      
        BEGIN
            --l_tableName := F_IDS_GET_TABLENAME('dsGpjyfx', I_KHH);
            --���׷���
            l_sqlBuf := "select" ||
                "   khh," ||
                "   0 as qmzzc," ||
                "   0 as qczzc," ||
                "   0 as qmzjye," ||
                "   0 as qmzqsz," ||
                "   0 as qmzfz," ||
                "   0 as yk," ||
                "   0 as ykl," ||
                "   0 as nhsyl," ||
                "   0 as crje," ||
                "   0 as qcje," ||
                "   0 as zrzqsz," ||
                "   0 as zczqsz," ||
                "   0 as zcjlr," ||
                "   0 as bdl," ||
                "   0 as zdhcl," ||
                "   0 as pjzzc," ||
                "   0 as pjsz," ||
                "   0 as byts," ||
                "   0 as zsz_gp," ||
                "   0 as yk_gp," ||
                "   0 as cggs_gp," ||
                "   0 as cgcgl_gp," ||
                "   cjje_gp_mr," ||
                "   cjje_gp_mc," ||
                "   jybs_gp_mr," ||
                "   jybs_gp_mc," ||
                "   ztcs_gp," ||
                "   ztsl_gp," ||
                "   ztcgl_gp," ||
                "   cast('' as string) as yl_list," ||
                "   cast('' as string) as ks_list," ||
                "   cast('' as string) as yl_zqpz_list," ||
                "   cast('' as string) as ks_zqpz_list," ||
                "   ztcgl," ||
                "   0 as xgcgl," ||
                "   cast('' as string) as sshy," ||
                "   ztcs," ||
                "   czcs," ||
                "   0 as qmzzc_jzjy," ||
                "   0 as qczzc_jzjy," ||
                "   0 as qmzqsz_jzjy," ||
                "   0 as qmzjye_jzjy," ||
                "   0 as yk_jzjy," ||
                "   0 as qmzxjz_jzjy," ||
                "   0 as zxjz_zzl_jzjy," ||
                "   0 as qmzzc_rzrq," ||
                "   0 as qczzc_rzrq," ||
                "   0 as qmzqsz_rzrq," ||
                "   0 as qmzjye_rzrq," ||
                "   0 as qmzfz_rzrq," ||
                "   0 as yk_rzrq," ||
                "   0 as qmzxjz_rzrq," ||
                "   0 as zxjz_zzl_rzrq," ||
                "   0 as qmzqsz_jrcp," ||
                "   0 as yk_jrcp," ||
                "   0 as qmzzc_ggqq," ||
                "   0 as qmzjye_ggqq," ||
                "   0 as qmzqsz_ggqq," ||
                "   0 as yk_ggqq," ||
                "   0 as qmzxjz_ggqq," ||
                "   0 as zxjz_zzl_ggqq from  " ||
                F_IDS_GET_TABLENAME('bill_sparkGpjyfx', I_KHH);
            
            l_sql := "INSERT INTO " || l_tableName || l_columns || l_sqlBuf;
            EXECUTE IMMEDIATE l_sql;
        END;

        BEGIN
            --l_tableName := F_IDS_GET_TABLENAME('dsGpyk', I_KHH);
            --��Ʊ�ֲ�ͳ��
            l_sqlBuf := "select" ||
                "   khh," ||
                "   0 as qmzzc," ||
                "   0 as qczzc," ||
                "   0 as qmzjye," ||
                "   0 as qmzqsz," ||
                "   0 as qmzfz," ||
                "   0 as yk," ||
                "   0 as ykl," ||
                "   0 as nhsyl," ||
                "   0 as crje," ||
                "   0 as qcje," ||
                "   0 as zrzqsz," ||
                "   0 as zczqsz," ||
                "   0 as zcjlr," ||
                "   0 as bdl," ||
                "   0 as zdhcl," ||
                "   0 as pjzzc," ||
                "   0 as pjsz," ||
                "   0 as byts," ||
                "   zsz_gp," ||
                "   yk_gp," ||
                "   cggs_gp," ||
                "   cgcgl_gp," ||
                "   0 as cjje_gp_mr," ||
                "   0 as cjje_gp_mc," ||
                "   0 as jybs_gp_mr," ||
                "   0 as jybs_gp_mc," ||
                "   0 as ztcs_gp," ||
                "   0 as ztsl_gp," ||
                "   0 as ztcgl_gp," ||
                "   cast('' as string) as yl_list," ||
                "   cast('' as string) as ks_list," ||
                "   cast('' as string) as yl_zqpz_list," ||
                "   cast('' as string) as ks_zqpz_list," ||
                "   0 as ztcgl," ||
                "   0 as xgcgl," ||
                "   cast('' as string) as sshy," ||
                "   0 as ztcs," ||
                "   0 as czcs," ||
                "   0 as qmzzc_jzjy," ||
                "   0 as qczzc_jzjy," ||
                "   0 as qmzqsz_jzjy," ||
                "   0 as qmzjye_jzjy," ||
                "   0 as yk_jzjy," ||
                "   0 as qmzxjz_jzjy," ||
                "   0 as zxjz_zzl_jzjy," ||
                "   0 as qmzzc_rzrq," ||
                "   0 as qczzc_rzrq," ||
                "   0 as qmzqsz_rzrq," ||
                "   0 as qmzjye_rzrq," ||
                "   0 as qmzfz_rzrq," ||
                "   0 as yk_rzrq," ||
                "   0 as qmzxjz_rzrq," ||
                "   0 as zxjz_zzl_rzrq," ||
                "   0 as qmzqsz_jrcp," ||
                "   0 as yk_jrcp," ||
                "   0 as qmzzc_ggqq," ||
                "   0 as qmzjye_ggqq," ||
                "   0 as qmzqsz_ggqq," ||
                "   0 as yk_ggqq," ||
                "   0 as qmzxjz_ggqq," ||
                "   0 as zxjz_zzl_ggqq from " ||
                F_IDS_GET_TABLENAME('bill_sparkGpYk', I_KHH);
                
            l_sql := "INSERT INTO " || l_tableName || l_columns || l_sqlBuf;
            EXECUTE IMMEDIATE l_sql;
        END;
        
        BEGIN
            --l_tableName := F_IDS_GET_TABLENAME('dsYklist', I_KHH);
            --ӯ�������б�
            l_sqlBuf := "select" ||
                "   khh," ||
                "   0 as qmzzc," ||
                "   0 as qczzc," ||
                "   0 as qmzjye," ||
                "   0 as qmzqsz," ||
                "   0 as qmzfz," ||
                "   0 as yk," ||
                "   0 as ykl," ||
                "   0 as nhsyl," ||
                "   0 as crje," ||
                "   0 as qcje," ||
                "   0 as zrzqsz," ||
                "   0 as zczqsz," ||
                "   0 as zcjlr," ||
                "   0 as bdl," ||
                "   0 as zdhcl," ||
                "   0 as pjzzc," ||
                "   0 as pjsz," ||
                "   0 as byts," ||
                "   0 as zsz_gp," ||
                "   0 as yk_gp," ||
                "   0 as cggs_gp," ||
                "   0 as cgcgl_gp," ||
                "   0 as cjje_gp_mr," ||
                "   0 as cjje_gp_mc," ||
                "   0 as jybs_gp_mr," ||
                "   0 as jybs_gp_mc," ||
                "   0 as ztcs_gp," ||
                "   0 as ztsl_gp," ||
                "   0 as ztcgl_gp," ||
                "   yl_list," ||
                "   ks_list," ||
                "   cast('' as string) as yl_zqpz_list," ||
                "   cast('' as string) as ks_zqpz_list," ||
                "   0 as ztcgl," ||
                "   xgcgl," ||
                "   cast('' as string) as sshy," ||
                "   0 as ztcs," ||
                "   0 as czcs," ||
                "   0 as qmzzc_jzjy," ||
                "   0 as qczzc_jzjy," ||
                "   0 as qmzqsz_jzjy," ||
                "   0 as qmzjye_jzjy," ||
                "   0 as yk_jzjy," ||
                "   0 as qmzxjz_jzjy," ||
                "   0 as zxjz_zzl_jzjy," ||
                "   0 as qmzzc_rzrq," ||
                "   0 as qczzc_rzrq," ||
                "   0 as qmzqsz_rzrq," ||
                "   0 as qmzjye_rzrq," ||
                "   0 as qmzfz_rzrq," ||
                "   0 as yk_rzrq," ||
                "   0 as qmzxjz_rzrq," ||
                "   0 as zxjz_zzl_rzrq," ||
                "   0 as qmzqsz_jrcp," ||
                "   0 as yk_jrcp," ||
                "   0 as qmzzc_ggqq," ||
                "   0 as qmzjye_ggqq," ||
                "   0 as qmzqsz_ggqq," ||
                "   0 as yk_ggqq," ||
                "   0 as qmzxjz_ggqq," ||
                "   0 as zxjz_zzl_ggqq from " ||
                F_IDS_GET_TABLENAME('bill_sparkYklist', I_KHH);
            
            l_sql := "INSERT INTO " || l_tableName || l_columns || l_sqlBuf;
            EXECUTE IMMEDIATE l_sql;
        END;
        
        BEGIN
            --l_tableName := F_IDS_GET_TABLENAME('dsYkzqpzlist', I_KHH);
            --��֤ȯƷ��ӯ������
            l_sqlBuf := "select" ||
                "   khh," ||
                "   0 as qmzzc," ||
                "   0 as qczzc," ||
                "   0 as qmzjye," ||
                "   0 as qmzqsz," ||
                "   0 as qmzfz," ||
                "   0 as yk," ||
                "   0 as ykl," ||
                "   0 as nhsyl," ||
                "   0 as crje," ||
                "   0 as qcje," ||
                "   0 as zrzqsz," ||
                "   0 as zczqsz," ||
                "   0 as zcjlr," ||
                "   0 as bdl," ||
                "   0 as zdhcl," ||
                "   0 as pjzzc," ||
                "   0 as pjsz," ||
                "   0 as byts," ||
                "   0 as zsz_gp," ||
                "   0 as yk_gp," ||
                "   0 as cggs_gp," ||
                "   0 as cgcgl_gp," ||
                "   0 as cjje_gp_mr," ||
                "   0 as cjje_gp_mc," ||
                "   0 as jybs_gp_mr," ||
                "   0 as jybs_gp_mc," ||
                "   0 as ztcs_gp," ||
                "   0 as ztsl_gp," ||
                "   0 as ztcgl_gp," ||
                "   cast('' as string) as yl_list," ||
                "   cast('' as string) as ks_list," ||
                "   yl_zqpz_list," ||
                "   ks_zqpz_list," ||
                "   0 as ztcgl," ||
                "   0 as xgcgl," ||
                "   cast('' as string) as sshy," ||
                "   0 as ztcs," ||
                "   0 as czcs," ||
                "   0 as qmzzc_jzjy," ||
                "   0 as qczzc_jzjy," ||
                "   0 as qmzqsz_jzjy," ||
                "   0 as qmzjye_jzjy," ||
                "   0 as yk_jzjy," ||
                "   0 as qmzxjz_jzjy," ||
                "   0 as zxjz_zzl_jzjy," ||
                "   0 as qmzzc_rzrq," ||
                "   0 as qczzc_rzrq," ||
                "   0 as qmzqsz_rzrq," ||
                "   0 as qmzjye_rzrq," ||
                "   0 as qmzfz_rzrq," ||
                "   0 as yk_rzrq," ||
                "   0 as qmzxjz_rzrq," ||
                "   0 as zxjz_zzl_rzrq," ||
                "   0 as qmzqsz_jrcp," ||
                "   0 as yk_jrcp," ||
                "   0 as qmzzc_ggqq," ||
                "   0 as qmzjye_ggqq," ||
                "   0 as qmzqsz_ggqq," ||
                "   0 as yk_ggqq," ||
                "   0 as qmzxjz_ggqq," ||
                "   0 as zxjz_zzl_ggqq from " ||
                F_IDS_GET_TABLENAME('bill_sparkYkZqpzList', I_KHH);
            
            l_sql := "INSERT INTO " || l_tableName || l_columns || l_sqlBuf;
            EXECUTE IMMEDIATE l_sql;
        END;
        
        BEGIN
            --l_tableName := F_IDS_GET_TABLENAME('dsHyph', I_KHH);
            --�ֲ�ƫ�÷���
            l_sqlBuf := "select" ||
                "   khh," ||
                "   0 as qmzzc," ||
                "   0 as qczzc," ||
                "   0 as qmzjye," ||
                "   0 as qmzqsz," ||
                "   0 as qmzfz," ||
                "   0 as yk," ||
                "   0 as ykl," ||
                "   0 as nhsyl," ||
                "   0 as crje," ||
                "   0 as qcje," ||
                "   0 as zrzqsz," ||
                "   0 as zczqsz," ||
                "   0 as zcjlr," ||
                "   0 as bdl," ||
                "   0 as zdhcl," ||
                "   0 as pjzzc," ||
                "   0 as pjsz," ||
                "   0 as byts," ||
                "   0 as zsz_gp," ||
                "   0 as yk_gp," ||
                "   0 as cggs_gp," ||
                "   0 as cgcgl_gp," ||
                "   0 as cjje_gp_mr," ||
                "   0 as cjje_gp_mc," ||
                "   0 as jybs_gp_mr," ||
                "   0 as jybs_gp_mc," ||
                "   0 as ztcs_gp," ||
                "   0 as ztsl_gp," ||
                "   0 as ztcgl_gp," ||
                "   cast('' as string) as yl_list," ||
                "   cast('' as string) as ks_list," ||
                "   cast('' as string) as yl_zqpz_list," ||
                "   cast('' as string) as ks_zqpz_list," ||
                "   0 as ztcgl," ||
                "   0 as xgcgl," ||
                "   sshy," ||
                "   0 as ztcs," ||
                "   0 as czcs," ||
                "   0 as qmzzc_jzjy," ||
                "   0 as qczzc_jzjy," ||
                "   0 as qmzqsz_jzjy," ||
                "   0 as qmzjye_jzjy," ||
                "   0 as yk_jzjy," ||
                "   0 as qmzxjz_jzjy," ||
                "   0 as zxjz_zzl_jzjy," ||
                "   0 as qmzzc_rzrq," ||
                "   0 as qczzc_rzrq," ||
                "   0 as qmzqsz_rzrq," ||
                "   0 as qmzjye_rzrq," ||
                "   0 as qmzfz_rzrq," ||
                "   0 as yk_rzrq," ||
                "   0 as qmzxjz_rzrq," ||
                "   0 as zxjz_zzl_rzrq," ||
                "   0 as qmzqsz_jrcp," ||
                "   0 as yk_jrcp," ||
                "   0 as qmzzc_ggqq," ||
                "   0 as qmzjye_ggqq," ||
                "   0 as qmzqsz_ggqq," ||
                "   0 as yk_ggqq," ||
                "   0 as qmzxjz_ggqq," ||
                "   0 as zxjz_zzl_ggqq from " ||
                F_IDS_GET_TABLENAME('bill_sparkHyph', I_KHH);
            
            l_sql := "INSERT INTO " || l_tableName || l_columns || l_sqlBuf;
            EXECUTE IMMEDIATE l_sql;
        END;
        
        --����khh�ϲ��������   
        BEGIN
            l_tableName := F_IDS_GET_TABLENAME('bill_sparkResult1', I_KHH);
            l_sqlBuf := "select" ||
                "   khh," ||
                "   sum(qmzzc) as qmzzc," ||
                "   sum(qczzc) as qczzc," ||
                "   sum(qmzjye) as qmzjye," ||
                "   sum(qmzqsz) as qmzqsz," ||
                "   sum(qmzfz) as qmzfz," ||
                "   sum(yk) as yk," ||
                "   sum(ykl) as ykl," ||
                "   sum(nhsyl) as nhsyl," ||
                "   sum(crje) as crje," ||
                "   sum(qcje) as qcje," ||
                "   sum(zrzqsz) as zrzqsz," ||
                "   sum(zczqsz) as zczqsz," ||
                "   sum(zcjlr) as zcjlr," ||
                "   sum(bdl) as bdl," ||
                "   sum(zdhcl) as zdhcl," ||
                "   sum(pjzzc) as pjzzc," ||
                "   sum(pjsz) as pjsz," ||
                "   sum(byts) as byts," ||
                "   sum(zsz_gp) as zsz_gp," ||
                "   sum(yk_gp) as yk_gp," ||
                "   sum(cggs_gp) as cggs_gp," ||
                "   sum(cgcgl_gp) as cgcgl_gp," ||
                "   sum(cjje_gp_mr) as cjje_gp_mr," ||
                "   sum(cjje_gp_mc) as cjje_gp_mc," ||
                "   sum(jybs_gp_mr) as jybs_gp_mr," ||
                "   sum(jybs_gp_mc) as jybs_gp_mc," ||
                "   sum(ztcs_gp) as ztcs_gp," ||
                "   sum(ztsl_gp) as ztsl_gp," ||
                "   sum(ztcgl_gp) as ztcgl_gp," ||
                "   concat_ws('',collect_set(yl_list)) as yl_list," ||
                "   concat_ws('',collect_set(ks_list)) as ks_list," ||
                "   concat_ws('',collect_set(yl_zqpz_list)) as yl_zqpz_list," ||
                "   concat_ws('',collect_set(ks_zqpz_list)) as ks_zqpz_list," ||
                "   sum(ztcgl) as ztcgl," ||
                "   sum(xgcgl) as xgcgl," ||
                "   concat_ws('',collect_set(sshy)) as sshy," ||
                "   sum(ztcs) as ztcs," ||
                "   sum(czcs) as czcs," ||
                "   sum(qmzzc_jzjy) as qmzzc_jzjy," ||
                "   sum(qczzc_jzjy) as qczzc_jzjy," ||
                "   sum(qmzqsz_jzjy) as qmzqsz_jzjy," ||
                "   sum(qmzjye_jzjy) as qmzjye_jzjy," ||
                "   sum(yk_jzjy) as yk_jzjy," ||
                "   sum(qmzxjz_jzjy) as qmzxjz_jzjy," ||
                "   sum(zxjz_zzl_jzjy) as zxjz_zzl_jzjy," ||
                "   sum(qmzzc_rzrq) as qmzzc_rzrq," ||
                "   sum(qczzc_rzrq) as qczzc_rzrq," ||
                "   sum(qmzqsz_rzrq) as qmzqsz_rzrq," ||
                "   sum(qmzjye_rzrq) as qmzjye_rzrq," ||
                "   sum(qmzfz_rzrq) as qmzfz_rzrq," ||
                "   sum(yk_rzrq) as yk_rzrq," ||
                "   sum(qmzxjz_rzrq) as qmzxjz_rzrq," ||
                "   sum(zxjz_zzl_rzrq) as zxjz_zzl_rzrq," ||
                "   sum(qmzqsz_jrcp) as qmzqsz_jrcp," ||
                "   sum(yk_jrcp) as yk_jrcp," ||
                "   sum(qmzzc_ggqq) as qmzzc_ggqq," ||
                "   sum(qmzjye_ggqq) as qmzjye_ggqq," ||
                "   sum(qmzqsz_ggqq) as qmzqsz_ggqq," ||
                "   sum(yk_ggqq) as yk_ggqq," ||
                "   sum(qmzxjz_ggqq) as qmzxjz_ggqq," ||
                "   sum(zxjz_zzl_ggqq) as zxjz_zzl_ggqq from  " ||
                F_IDS_GET_TABLENAME('bill_sparkResult0', I_KHH) || " group by khh";
                
            F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
        END;
        
        --���˵�����
        IF I_BILLSTYPE = 'year' THEN
            --Ͷ�ʷ�ʽ��Ͷ���㼣ͳ��
            BEGIN
                l_tableName := F_IDS_GET_TABLENAME('bill_sparkNzdtz', I_KHH);
                l_sqlBuf := "select " ||
                            "   khh," ||
                            "   sum(cxtzgs) as cxtzgs," ||
                            "   sum(zxtzgs) as zxtzgs," ||
                            "   concat_ws('',collect_set(ykzj)) as ykzj" ||
                            " from  " ||
                            "(select" ||
                            "   khh," ||
                            "   cxtzgs," ||
                            "   zxtzgs," ||
                            "   cast('' as string) as ykzj" ||
                            " from  " ||
                            F_IDS_GET_TABLENAME('bill_sparkTzfs', I_KHH)  ||
                            " union all" ||
                            " select" ||
                            "   khh," ||
                            "   0 as cxtzgs," ||
                            "   0 as zxtzgs," ||
                            "   ykzj" ||
                            " from  " ||
                            F_IDS_GET_TABLENAME('bill_sparkTzzj', I_KHH) || ") a group by khh";
                
                F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
            END;
        END IF;
        
        --���ݻ��ܺϲ�
        BEGIN
            l_tableName := F_IDS_GET_TABLENAME('bill_sparkZd', I_KHH);
            l_sqlBuf := "select " ||
                    "   z.khh," ||
                    "   z.qmzzc," ||
                    "   z.qczzc," ||
                    "   z.qmzjye," ||
                    "   z.qmzqsz," ||
                    "   z.qmzfz," ||
                    "   z.yk,"    ||
                    "   z.ykl,"   ||
                    "   z.nhsyl," ||
                    "   z.crje," ||
                    "   z.qcje," ||
                    "   z.zrzqsz," ||
                    "   z.zczqsz," ||
                    "   z.zcjlr," ||
                    "   z.bdl," ||
                    "   z.zdhcl," ||
                    "   rank() over(partition by '' order by ykl desc nulls last) as ykl_pm," ||
                    "   1-rank() over(partition by '' order by ykl desc nulls last)/count(1) over(partition by '') as ykl_pm_ratio," ||
                    "   case when z.pjzzc=0 then 0 else nvl(z.zsz_gp,0)/z.byts/z.pjzzc end as cw_gp," ||
                    "   case when z.pjzzc=0 then 0 else (nvl(z.cjje_gp_mr,0) + nvl(z.cjje_gp_mc,0))/z.byts/z.pjzzc end as hsl_gp," ||
                    "   z.yk_gp,"    ||
                    "   z.cggs_gp,"  ||
                    "   z.cgcgl_gp," ||
                    "   z.cjje_gp_mr," ||
                    "   z.cjje_gp_mc," ||
                    "   z.jybs_gp_mr," ||
                    "   z.jybs_gp_mc," ||
                    "   z.ztcs_gp," ||
                    "   z.ztsl_gp," ||
                    "   z.ztcgl_gp,"||
                    "   z.yl_list," ||
                    "   z.ks_list," ||
                    "   z.yl_zqpz_list," ||
                    "   z.ks_zqpz_list," ||
                    "   rank() over(partition by '' order by z.yk desc nulls last) as sy_rank," ||
                    "   rank() over(partition by '' order by z.ztcgl desc nulls last) as ztcgl_rank," ||
                    "   rank() over(partition by '' order by z.zdhcl desc nulls last) as zdhcl_rank," ||
                    "   rank() over(partition by '' order by z.xgcgl desc nulls last) as xgcgl_rank," ||
                    "   concat(nvl(z.sshy,'������ҵƫ��'), ','," || --��ҵƫ��
                    "          case when if(z.pjzzc=0,0,z.pjsz/z.pjzzc)=0 then '�ղ�'" || --��λ
                    "           when if(z.pjzzc=0,0,z.pjsz/z.pjzzc)<0.1 then '�����'" ||
                    "           when if(z.pjzzc=0,0,z.pjsz/z.pjzzc)<=0.4 then '���'" ||
                    "           when if(z.pjzzc=0,0,z.pjsz/z.pjzzc)<=0.6 then '���'" ||
                    "           when if(z.pjzzc=0,0,z.pjsz/z.pjzzc)<=0.9 then '�ز�'" ||
                    "          else '����' end, ','," ||
                    "          case when z.ztcs>=10 then '��T����' when z.ztcs>=5 then '������T' when z.ztcs>=2 then '����T' else '������T' end,','," || --��T
                    IF(I_BILLSTYPE = 'year', " case when n.cxtzgs>0 then '����Ͷ��' when n.zxtzgs>0 then '����Ͷ��' else '����Ͷ��' end,','," , "")  || --Ͷ�ʷ�ʽ
                    "          case when z.czcs is null then '��Ƶ' when z.czcs/z.byts<0.25 then '��Ƶ' when z.czcs/z.byts<=0.7 then '��Ƶ' else '��Ƶ' end" || --����Ƶ��
                    " )  as tzgjz," ||
                    IF(I_BILLSTYPE = 'year', "n.ykzj as ykzj,", "") || --ӯ���㼣
                    "   z.qmzzc_jzjy," ||
                    "   z.qczzc_jzjy," ||
                    "   z.qmzqsz_jzjy," ||
                    "   z.qmzjye_jzjy," ||
                    "   z.yk_jzjy,"     ||
                    "   z.qmzxjz_jzjy,"   ||
                    "   z.zxjz_zzl_jzjy," ||
                    "   z.qmzzc_rzrq," ||
                    "   z.qczzc_rzrq," ||
                    "   z.qmzqsz_rzrq," ||
                    "   z.qmzjye_rzrq," ||
                    "   z.qmzfz_rzrq,"  ||
                    "   z.yk_rzrq,"     ||
                    "   z.qmzxjz_rzrq,"   ||
                    "   z.zxjz_zzl_rzrq," ||
                    "   z.qmzqsz_jrcp,"   ||
                    "   z.yk_jrcp,"     ||
                    "   z.qmzzc_ggqq,"  ||
                    "   z.qmzjye_ggqq," ||
                    "   z.qmzqsz_ggqq," ||
                    "   z.yk_ggqq,"     ||
                    "   z.qmzxjz_ggqq,"    ||
                    "   z.zxjz_zzl_ggqq"   ||
                    " from  " || F_IDS_GET_TABLENAME('bill_sparkResult1', I_KHH) ||" z " || 
                    IF(I_BILLSTYPE = 'year', " left join "|| F_IDS_GET_TABLENAME('bill_sparkNzdtz', I_KHH) || "n on z.khh = n.khh", "");
                    
            F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);            
        END;
    END;
    
    /*
     * �������Ŀ���
     */
    BEGIN
        IF I_BILLSTYPE = 'year' THEN
            l_tableName := 't_stat_zd_n';
        ELSE
            l_tableName := 't_stat_zd_y';
        END IF;
        
        F_IDS_OVERWRITE_PARTITION(F_IDS_GET_TABLENAME('bill_sparkZd', I_KHH), "CUST", l_tableName , l_sj, I_KHH);
    END;
END;
/