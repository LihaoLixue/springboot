CREATE OR REPLACE PROCEDURE CUST.P_IDS_CUST_INVESTMENT_DIAGNOSIS(
                            i_ksrq in int,--开始日期
							i_jsrq in int,--结束日期
                            i_khh in STRING
)is
/*********************************************************************************************
    *文件名称：CUST.P_IDS_CUST_INVESTMENT_DIAGNOSIS
    *项目名称：IDS计算
    *文件说明：投资诊断

    创建人：王睿驹
    功能说明：投资诊断
    
    修改者            版本号            修改日期            说明
    王睿驹            v1.0.0            2019/6/20            创建
*********************************************************************************************/
DECLARE 
	l_dbname STRING default "sparktemp.";--表前缀
    l_suffix STRING;--表后缀
	l_sqlBuf STRING;
    l_sqlWhere STRING;
	l_zsdm STRING default "399300";
	l_syrq INT;--开始日期的上个交易日
	l_qmrq INT;--期末日期	
	l_zssyl decimal(12,6);
	l_syl decimal(12,6);--收益率
	l_ma5Date INT;--开始日期往前推5个交易日
	l_tableName_sparkRzd STRING;--日账单
	--l_tableName_sparkZqlb STRING;--证券类别
	l_tableName_sparkGsgk STRING;--公司概况
	l_tableName_sparkJzjyJgls STRING;--大集中交割
	l_tableName_sparkXyJgls STRING;--两融交割
	l_tableName_sparkJzjyZqye STRING;--大集中持仓
	l_tableName_sparkXyZqye STRING;	--两融持仓
	l_tableName_sparkJzjyTzsy STRING;--大集中清仓
	l_tableName_sparkXyTzsy STRING;--两融清仓
	l_tableName_sparkTzfb STRING;--1、投资分布
	l_tableName_sparkHqma STRING;--生产带有5日均线的行情数据
	l_tableName_sparkGpfl STRING;--个股大小盘、价值股分类
	l_tableName_sparksly STRING;--收益率
	l_tableName_sparkzssly STRING;
	l_tableName_sparkzsbd STRING;
	l_tableName_sparkZcfx STRING;
	l_noRiskRate decimal(12,6) default 0.04;
BEGIN
	--初始化
	BEGIN
		SELECT F_GET_JYR_DATE(i_ksrq,-1) INTO l_syrq FROM SYSTEM.dual;
		SELECT F_GET_JYR_DATE(i_jsrq,0) INTO l_qmrq FROM SYSTEM.dual;
		SELECT F_GET_JYR_DATE(i_jsrq,-5) INTO l_ma5Date FROM SYSTEM.dual;
		IF i_khh IS NULL  THEN
			l_suffix :='';
		ELSE
			l_suffix :='_'||i_khh;
			l_sqlWhere:=' and khh='||i_khh;
		END IF;
			--日账单
		BEGIN
			l_tableName_sparkRzd :=l_dbname||'zhfx_sparkzcZqsz'||l_suffix;
			l_sqlBuf:="select * from cust.t_stat_zd_r where rq between "||i_ksrq||" and "||i_jsrq||l_sqlWhere;
			F_IDS_CREATE_TEMP_TABLE(l_sqlBuf,l_tableName_sparkRzd);
		END;
	END;
	
	--getIndexFluctuation获取指数的收益
	BEGIN	
		l_sqlBuf:="SELECT CAST(1.0 AS DECIMAL(12,6)) as zssyl,1 as start,1 as end,'1' as type FROM "||l_tableName_sparkRzd||"  WHERE 1=0";
		l_tableName_sparkzsbd:=l_dbname||"IF_sparkzsbd"||l_suffix;
		F_IDS_CREATE_TEMP_TABLE(l_sqlBuf,l_tableName_sparkzsbd);
		DECLARE 
			CURSOR c_zsbd(v_qmrq INT,v_zsdm STRING) is select start,end from info.tfx_zsbd@dblink1 where rq=v_qmrq and zsdm=v_zsdm;
			l_qcz DECIMAL(12,6);--期初值
			l_qmz DECIMAL(12,6);--期末值
			v_start INT;
			v_end INT;
			v_type STRING;
		BEGIN
			OPEN c_zsbd(l_qmrq,l_zsdm);
			LOOP FETCH c_zsbd INTO  v_start,v_end;
				EXIT WHEN c_zsbd%NOTFOUND;
				select cast(zxj AS DECIMAL(12,6)) into l_qcz from info.this_zshq where zsdm=l_zsdm and rq=F_GET_JYR_DATE(v_start,-1);
				select cast(zxj AS DECIMAL(12,6)) into l_qmz from info.this_zshq where zsdm=l_zsdm and rq=v_end;
				l_syl:=l_qmz/l_qcz-1;
				EXECUTE IMMEDIATE "INSERT INTO "||l_tableName_sparkzsbd||" SELECT "||l_syl||","||v_start||","||v_end||","||v_type||", FROM "||l_tableName_sparkRzd||" LIMIT 1";
			END LOOP;
		END;				 
		select cast(zxj AS DECIMAL(12,6)) into l_qcz from info.this_zshq where zsdm=l_zsdm and rq=F_GET_JYR_DATE(i_ksrq,-1);
		select cast(zxj AS DECIMAL(12,6)) into l_qmz from info.this_zshq where zsdm=l_zsdm and rq=l_qmrq;
		l_zssyl:=l_qmz/l_qcz-1;
	END;
	
	--loadSourceDatas加载源数据
	BEGIN
		
		--证券类别
		--BEGIN
		--	l_tableName_sparkZqlb :=l_dbname||'zhfx_sparkZqlb'||l_suffix;
		--	l_sqlBuf:="select * from DSC_CFG.VW_T_ZQLB_IDS";
		--	F_IDS_CREATE_TEMP_TABLE(l_sqlBuf,l_tableName_sparkZqlb);
		--END;
		--公司概况
		BEGIN
			l_tableName_sparkGsgk :=l_dbname||'zhfx_sparkGsgk'||l_suffix;
			l_sqlBuf:="select jys,zqdm,sshy from (select *,row_number() over(partition by jys,zqdm order by id desc) rn from info.tgp_gsgk where length(sshy)>0 and sshy!='null' and sshy!='无') where rn=1";
			F_IDS_CREATE_TEMP_TABLE(l_sqlBuf,l_tableName_sparkGsgk);
		END;
		--大集中交割
		BEGIN
			l_tableName_sparkJzjyJgls :=l_dbname||'zhfx_sparkJzjyJgls'||l_suffix;
			l_sqlBuf:="select * from cust.t_jgmxls_his_qs  where cjrq between "||i_ksrq||" and "||i_jsrq||l_sqlWhere;
			F_IDS_CREATE_TEMP_TABLE(l_sqlBuf,l_tableName_sparkJzjyJgls);
		END;
		--两融交割
		BEGIN
			l_tableName_sparkXyJgls :=l_dbname||'zhfx_sparkXyJgls'||l_suffix;
			l_sqlBuf:="select * from cust.t_xy_jgmxls_his_qs where cjrq between "||i_ksrq||" and "||i_jsrq||l_sqlWhere;
			F_IDS_CREATE_TEMP_TABLE(l_sqlBuf,l_tableName_sparkXyJgls);
		END;
		--大集中持仓
		BEGIN
			l_tableName_sparkJzjyZqye :=l_dbname||'zhfx_sparkJzjyZqye'||l_suffix;
			l_sqlBuf:="select * from cust.t_zqye_his where rq between "||i_ksrq||" and "||i_jsrq||l_sqlWhere;
			F_IDS_CREATE_TEMP_TABLE(l_sqlBuf,l_tableName_sparkJzjyZqye);
		END;
		--两融持仓
		BEGIN
			l_tableName_sparkXyZqye :=l_dbname||'zhfx_sparkXyZqye'||l_suffix;
			l_sqlBuf:="select * from cust.t_xy_zqye_his where rq between "||i_ksrq||" and "||i_jsrq||l_sqlWhere;
			F_IDS_CREATE_TEMP_TABLE(l_sqlBuf,l_tableName_sparkXyZqye);
		END;
		--大集中清仓
		BEGIN
			l_tableName_sparkJzjyTzsy :=l_dbname||'zhfx_sparkJzjyTzsy'||l_suffix;
			l_sqlBuf:="select * from cust.t_tzsy where qcrq between "||i_ksrq||" and "||i_jsrq||l_sqlWhere;
			F_IDS_CREATE_TEMP_TABLE(l_sqlBuf,l_tableName_sparkJzjyTzsy);
		END;
		--两融清仓
		BEGIN
			l_tableName_sparkXyTzsy :=l_dbname||'zhfx_sparkXyTzsy'||l_suffix;
			l_sqlBuf:="select * from cust.t_tzsy where qcrq between "||i_ksrq||" and "||i_jsrq||l_sqlWhere;
			F_IDS_CREATE_TEMP_TABLE(l_sqlBuf,l_tableName_sparkXyTzsy);
		END;
		--1、投资分布
		BEGIN
			l_tableName_sparkTzfb :=l_dbname||'zhfx_sparkTzfb'||l_suffix;		
			l_sqlBuf: = "select
			   khh,
			   z.rq,
			   z.jys,
			   z.zqdm,
			   nvl(z.zqmc,z.zqdm) as zqmc,
			   dryk,
			   case when z.lb='fp' then '理财' when z.lb='so' then '期权' else ifnull(a.zqpzmc,'股票') end as zqpz,
			   regexp_replace(c.sshy,'[,; ]','') as sshy,
			   z.lb,
			   z.fdyk,
			   z.zxsz,
			   z.cccb
			 from
			(select khh,z.rq,z.jys,z.zqdm,d.zqmc,tranCurrency(cast(dryk as double),z.bz) as dryk,z.zqlb,'jzjy' as lb,tranCurrency(cast(zxsz-tbcccb as double),z.bz) as fdyk,tranCurrency(cast(zxsz as double),z.bz) as zxsz,tranCurrency(cast(cccb as double),z.bz) as cccb
			  from "||l_tableName_sparkJzjyZqye||" z left join cust.t_zqdm d on (z.jys=d.jys and z.zqdm=d.zqdm)
			 union ALL
			 select khh,z.qcrq as rq,z.jys,z.zqdm,d.zqmc,tranCurrency(cast(dryk as double),z.bz) as dryk,z.zqlb,'jzjy' as lb,0.0 as fdyk,0.0 as zxsz,0.0 as cccb
			  from  "||l_tableName_sparkJzjyTzsy||"  z left join cust.t_zqdm d on (z.jys=d.jys and z.zqdm=d.zqdm)
			 union all
			 select khh,z.rq,z.jys,z.zqdm,d.zqmc,tranCurrency(cast(dryk as double),z.bz) as dryk,z.zqlb,'rzrq' as lb,tranCurrency(cast(zxsz-tbcccb as double),z.bz) as fdyk,tranCurrency(cast(zxsz as double),z.bz) as zxsz,tranCurrency(cast(cccb as double),z.bz) as cccb
			  from "||l_tableName_sparkXYzqye||" z left join cust.t_zqdm d on (z.jys=d.jys and z.zqdm=d.zqdm)
			 union ALL
			 select khh,z.qcrq as rq,z.jys,z.zqdm,d.zqmc,tranCurrency(cast(dryk as double),z.bz) as dryk,z.zqlb,'rzrq' as lb,0.0 as fdyk,0.0 as zxsz,0.0 as cccb
			  from "||l_tableName_sparkXYtzsy||" z left join cust.t_zqdm d on (z.jys=d.jys and z.zqdm=d.zqdm))z
			 left join"||F_IDS_GET_TABLENAME('sparkZqlb',i_khh)||" a on (z.zqlb=a.zqlb and z.jys=a.jys)
			 left join "||l_tableName_sparkGsgk||" c on (z.jys=c.jys and z.zqdm=c.zqdm)";
			 F_IDS_CREATE_TEMP_TABLE(l_sqlBuf,l_tableName_sparkTzfb);
		END;	 
		--生产带有5日均线的行情数据
		BEGIN
			l_tableName_sparkHqma :=l_dbname||'zhfx_sparkHqma';
			l_sqlBuf:= "select jys,zqdm,rq,zxj,ma5,ma5*1.03 as ma5upper,ma5*0.97 as ma5lowwer from
				(select jys,
				   zqdm,
				   rq,
				   zxj,
				   avg(zxj) over(partition by jys,zqdm order by rq asc rows between 4 preceding and current row) as ma5
				 from cust.t_zqhq_his where rq between "||l_ma5Date|| " and " ||i_jsrq|| ") a where rq between " ||i_ksrq|| " and " ||i_jsrq;
			F_IDS_CREATE_TEMP_TABLE(l_sqlBuf,l_tableName_sparkHqma);
		END;
		--个股大小盘、价值股分类
		BEGIN
			l_tableName_sparkGpfl :=l_dbname||'zhfx_sparkGpfl';
			l_sqlBuf:= "select case when ltag>=10000000000 then '大盘股' when ltag>500000000 and ltag<10000000000 then '中盘股' else '小盘股' end as dxp,a.* from info.tgp_gsgk s,
				(select case when g.ggjtsyl=0 then '平衡' when g.ggjtsyl/j.jtsyl-1 <= -0.05 then '价值' when g.ggjtsyl/j.jtsyl-1>=0.05 then '成长' else '平衡' end as lx,
				g.zqdm,g.zqmc,g.zjhdlmc,g.ggjtsyl,j.jtsyl from info.TGP_HYGS g,  info.THY_JTSYL j where g.zjhdldm=j.hydm and ifnull(j.jtsyl,0) != 0) a
				where s.zqdm=a.zqdm";
			F_IDS_CREATE_TEMP_TABLE(l_sqlBuf,l_tableName_sparkGpfl);
		END;
				
	END;
	--positionAnalyze持仓分析
	BEGIN
		--选股能力
		l_sqlBuf:="select 
			   khh,
			   if(cggs_gp=0,0,cgcgs_gp/cggs_gp) as cgcgl_gp,
			   cggs_gp,
			   yl_gp,
			   ks_gp,
			   if(ks_gp=0,0,abs(yl_gp)/abs(ks_gp)) ykb
			 from 
			(select 
			   khh,
			   sum(case when ljyk>0 then 1 else 0 end) as cgcgs_gp,
			   count(distinct concat(jys,zqdm)) as cggs_gp,
			   sum(case when ljyk>0 then ljyk else 0 end) as yl_gp,
			   sum(case when ljyk<0 then ljyk else 0 end) as ks_gp
			 from
			(select 
			   khh,
			   jys,
			   zqdm,
			   sum(dryk) as ljyk
			 from "||l_tableName_sparkHqma||" where zqpz='股票' group by khh,jys,zqdm)a group by khh)a ";
		F_IDS_CREATE_TEMP_TABLE(l_sqlBuf,F_IDS_GET_TABLENAME('sparkGpYk',i_khh));
		--长短线偏好
		l_sqlBuf:= "select 
			   khh,
			   rq,
			   jys,
			   zqdm,
			   getTradeDate(rq,1) as qyr,
			   getTradeDate(rq,1) as hyr,
			   lag(rq,1,rq) over(partition by khh,jys,zqdm order by rq) as ccqyr,
			   lead(rq,1,rq) over(partition by khh,jys,zqdm order by rq) as cchyr
			 from "||l_tableName_sparkHqma||" where zqpz='股票'";
		F_IDS_CREATE_TEMP_TABLE(l_sqlBuf,F_IDS_GET_TABLENAME('sparkCgcxx_0',i_khh));	 
		l_sqlBuf:= "select
			   khh,
			   jys,
			   zqdm,
			   sum(case when (ccqyr=rq) or (cchyr=rq) or (ccqyr=qyr and cchyr=hyr) then 1 else 0 end)/count(1) as pjcgts
			 from "||F_IDS_GET_TABLENAME('sparkCgcxx_0',i_khh)||" group by khh,jys,zqdm";
		F_IDS_CREATE_TEMP_TABLE(l_sqlBuf,F_IDS_GET_TABLENAME('sparkCgcxx_1',i_khh));	 
		l_sqlBuf:="select 
			   khh,
			   sum(case when pjcgts<=7 then 1 else 0 end) as dxcg,
			   sum(case when pjcgts>7 and pjcgts<57 then 1 else 0 end) as zxcg,
			   sum(case when pjcgts>=57 then 1 else 0 end) as cxcg
			 from "||F_IDS_GET_TABLENAME('sparkCgcxx_1',i_khh)||" group by khh";
		F_IDS_CREATE_TEMP_TABLE(l_sqlBuf,F_IDS_GET_TABLENAME('sparkCgcxx',i_khh));	 
		--投资风格-大小盘/价值股偏好
		l_sqlBuf:="select
			   khh,
			   sum(zzc) as zzc
			 from"||F_IDS_GET_TABLENAME('sparkRzd',i_khh)||" z where rq>" || l_syrq || " group by khh";
		F_IDS_CREATE_TEMP_TABLE(l_sqlBuf,F_IDS_GET_TABLENAME('sparkTzfg_1',i_khh));	
		l_sqlBuf:= "select
			   khh,
			   concat_ws(';',collect_set(concat('dxp:',dxp,',',
			                                    'lx:',lx,',',
			                                    'cw:',cast(cw as string)) )) as tzfg
			 from 
			(select 
			   t.khh,
			   t.dxp,
			   t.lx,
			   cast(if(j.zzc=0,0,zsz/zzc) as decimal(9,6)) as cw
			 from"||F_IDS_GET_TABLENAME('sparkTzfg_1',i_khh)||" t,sparkRzcTj j where t.khh=j.khh)a group by khh";
		F_IDS_CREATE_TEMP_TABLE(l_sqlBuf,F_IDS_GET_TABLENAME('sparkTzfg',i_khh));	
		--投资风格-行业偏好
		l_sqlBuf:= "select 
			   khh,
			   sshy,
			   zsz
			 from
			(select
			   khh,
			   sshy,
			   zsz,
			   row_number() over(partition by khh order by zsz desc) rn
			 from
			(select
			   khh,
			   sshy,
			   sum(zxsz) zsz
			 from "||l_tableName_sparkHqma||" where zqpz='股票' group by khh,sshy)a)a where rn<=5";
		F_IDS_CREATE_TEMP_TABLE(l_sqlBuf,F_IDS_GET_TABLENAME('sparkHyph_1',i_khh));
		l_sqlBuf:= "select
			   khh,
			   sortProfitList(concat_ws(';',collect_set(concat('hy:',sshy,',',
			                                    'cw:',cast(cw as string)))),'cw') as hyph
			 from
			(select
			   h.khh,
			   h.sshy,
			   cast(if(j.zzc=0,0,zsz/zzc) as decimal(9,6)) as cw
			 from"||F_IDS_GET_TABLENAME('sparkHyph_1',i_khh)||" h,sparkRzcTj j where h.khh=j.khh)a group by khh";
		F_IDS_CREATE_TEMP_TABLE(l_sqlBuf,F_IDS_GET_TABLENAME('sparkHyph',i_khh));
		l_sqlBuf:= "select
			   khh,
			   jys,
			   zqdm,
			   zqmc,
			   zsz
			 from 
			(select 
			   khh,
			   jys,
			   zqdm,
			   zqmc,
			   zsz,
			   row_number() over(partition by khh order by zsz desc) rn
			 from
			(select
			   khh,
			   jys,
			   zqdm,
			   zqmc,
			   sum(zxsz) as zsz
			 from "||l_tableName_sparkHqma||" where zqpz='股票' group by khh,jys,zqdm,zqmc)a)a where rn<=5";
		F_IDS_CREATE_TEMP_TABLE(l_sqlBuf,F_IDS_GET_TABLENAME('sparkGgph_1',i_khh));
		l_sqlBuf:= "select
			   khh,
			   sortProfitList(concat_ws(';',collect_set(concat('jys:',jys,',',
			                                    'zqdm:',zqdm,',',
			                                    'zqmc:',zqmc,',',
			                                    'cw:',cast(cw as string)))),'cw') as ggph
			 from 
			(select
			   g.khh,
			   g.jys,
			   g.zqdm,
			   g.zqmc,
			   cast(if(j.zzc=0,0,zsz/zzc) as decimal(9,6)) as cw
			 from"||F_IDS_GET_TABLENAME('sparkGgph_1',i_khh)||" g,sparkRzcTj j where g.khh=j.khh)a group by khh";
		F_IDS_CREATE_TEMP_TABLE(l_sqlBuf,F_IDS_GET_TABLENAME('sparkGgph',i_khh));	 
		
	END;
	--securityTrade交易分析
	BEGIN
		l_sqlBuf:= "select z.khh,z.cjrq,z.jys,z.zqdm,z.zqlb,z.jylb,z.ysje,z.yssl,z.zxj,z.cjjg,z.lb,a.zqpzmc,regexp_replace(b.sshy,'[,; ]','') as sshy
			 from (select khh,cjrq,jys,zqdm,zqlb,jylb,ysje,yssl,zxj,cjjg,'jzjy' as lb from"||F_IDS_GET_TABLENAME('sparkJzjyJgls',i_khh)||"
			    union all 
			   select khh,cjrq,jys,zqdm,zqlb,jylb,ysje,yssl,zxj,cjjg,'rzrq' as lb from"||F_IDS_GET_TABLENAME('sparkXyJgls',i_khh)||") z
			 left join"||F_IDS_GET_TABLENAME('sparkZqlb',i_khh)||" a on (z.zqlb=a.zqlb and z.jys=a.jys)
			 left join"||F_IDS_GET_TABLENAME('sparkGsgk',i_khh)||" b on (z.jys=b.jys and z.zqdm=b.zqdm)";
		F_IDS_CREATE_TEMP_TABLE(l_sqlBuf,F_IDS_GET_TABLENAME('sparkJgls',i_khh));	 
		l_sqlBuf:= "select 
			   khh,
			   jys,
			   zqdm,
			   lb,
			   zqpzmc,
			   sshy,
			   sum(case when jylb='1' then 1 else 0 end) as mrcs,
			   sum(case when jylb='2' then 1 else 0 end) as mccs,
			   sum(case when jylb='1' then abs(ysje) else 0 end) as mrje,
			   sum(case when jylb='2' then abs(ysje) else 0 end) as mcje,
			   sum(case when jylb='1' then abs(yssl) else 0 end) as mrsl,
			   sum(case when jylb='2' then abs(yssl) else 0 end) as mcsl,
			   sum(case when (jylb='1' and cjjg<zxj) or (jylb='2' and cjjg>zxj) then 1 else 0 end) jycg,
			   count(1) as czcs,
			   sum(abs(ysje)) as jyl,
			   cjrq
			 from"||F_IDS_GET_TABLENAME('sparkJgls',i_khh)||" group by khh,jys,zqdm,lb,cjrq,zqpzmc,sshy";
		F_IDS_CREATE_TEMP_TABLE(l_sqlBuf,F_IDS_GET_TABLENAME('sparkJyfx1',i_khh));	
		l_sqlBuf:="select 
			   khh,
			   sum(case when zqpzmc='股票' then czcs else 0 end) as czcs,
			   sum(case when zqpzmc='股票' and mrsl>0 and mcsl>0 then 1 else 0 end) as ztcs_gp,
			   sum(case when zqpzmc='股票' and mrsl>0 and mcsl>0 then if((mcje/mcsl)>(mrje/mrsl),1,0) else 0 end) / 
			      sum(case when zqpzmc='股票' and mrsl>0 and mcsl>0 then 1 else 0 end) as ztcgl_gp,
			   sum(case when zqpzmc='股票' then jycg else 0 end) / sum(case when zqpzmc='股票' then czcs else 0 end) as zscgl
			 from"||F_IDS_GET_TABLENAME('sparkJyfx1',i_khh)||" group by khh";
		F_IDS_CREATE_TEMP_TABLE(l_sqlBuf,F_IDS_GET_TABLENAME('sparkJyfx2',i_khh));		 
		--择时能力
		l_sqlBuf:="select
			   khh,
			   czcs,
			   ztcs_gp,
			   ztcgl_gp,
			   zscgl
			 from"||F_IDS_GET_TABLENAME('sparkJyfx2',i_khh)||"";
		F_IDS_CREATE_TEMP_TABLE(l_sqlBuf,F_IDS_GET_TABLENAME('sparkGpjyfx',i_khh));		 
		l_sqlBuf:="select
			   j.khh,
			   sum(case when j.jylb='1' and j.cjjg<=h.ma5lowwer then 1 else 0 end) as zcjy_dx,
			   sum(case when j.jylb='2' and j.cjjg>=h.ma5upper then 1 else 0 end) as zcjy_gp,
			   sum(case when j.jylb='1' and j.cjjg>=h.ma5upper then 1 else 0 end) as ycjy_zz,
			   sum(case when j.jylb='2' and j.cjjg<=h.ma5lowwer then 1 else 0 end) as ycjy_sd
			 from"||F_IDS_GET_TABLENAME('sparkJgls',i_khh)||" j,sparkHqma h 
			 where j.cjrq=h.rq and j.jys=h.jys and j.zqdm=h.zqdm and j.zqpzmc='股票'
			 group by j.khh";
		F_IDS_CREATE_TEMP_TABLE(l_sqlBuf,F_IDS_GET_TABLENAME('sparkZycjy',i_khh));		 
	END;
	
	--mergeData
	BEGIN
		l_sqlBuf:= "select 
			   khh,
			   syl,
			   zdhcl,
			   hc_bq,
			   hchf,
			   bdl,
			   bdl_bq,
			   sharp,
			   calma,
			   beta,
			   pmnl_pf,
			   pmnl_pj,
			   pmnl,
			   pmnl_zs,
			   dpsyl,
			   syl_db_dp,
			   ylnl_pf,
			   ylnl_pj,
			   fknl_pf,
			   fknl_pj,
			   getFxbx(bdl_bq, hc_bq) as fxbx,
			   0 as cgcgl_gp,
			   0 as cggs_gp,
			   0 as yl_gp,
			   0 as ks_gp,
			   0 as ykb,
			   0 as dxcg,
			   0 as zxcg,
			   0 as cxcg,
			   0 as czcs,
			   0 as ztcs_gp,
			   0 as ztcgl_gp,
			   0 as zscgl,
			   0 as zcjy,
			   0 as ycjy,
			   '' as tzfg,
			   '' as hyph,
			   '' as ggph
			 from"||F_IDS_GET_TABLENAME('sparkZcfx',i_khh)||"";
		F_IDS_CREATE_TEMP_TABLE(l_sqlBuf,F_IDS_GET_TABLENAME('sparkdsZcfx',i_khh));	
		l_sqlBuf:=	  "select 
			   khh,
			   0 as syl,
			   0 as zdhcl,
			   '' as hc_bq,
			   '' as hchf,
			   0 as bdl,
			   '' as bdl_bq,
			   0 as sharp,
			   0 as calma,
			   0 as beta,
			   0 as pmnl_pf,
			   '' as pmnl_pj,
			   '' as pmnl,
			   '' as pmnl_zs,
			   0 as dpsyl,
			   0 as syl_db_dp,
			   0 as ylnl_pf,
			   '' as ylnl_pj,
			   0 as fknl_pf,
			   '' as fknl_pj,
			   '' as fxbx,
			   cgcgl_gp,
			   cggs_gp,
			   yl_gp,
			   ks_gp,
			   ykb,
			   0 as dxcg,
			   0 as zxcg,
			   0 as cxcg,
			   0 as czcs,
			   0 as ztcs_gp,
			   0 as ztcgl_gp,
			   0 as zscgl,
			   0 as zcjy,
			   0 as ycjy,
			   '' as tzfg,
			   '' as hyph,
			   '' as ggph
			 from"||F_IDS_GET_TABLENAME('sparkGpyk',i_khh)||"";
		F_IDS_CREATE_TEMP_TABLE(l_sqlBuf,F_IDS_GET_TABLENAME('sparkdsGpyk',i_khh));	 
		l_sqlBuf:=	"select 
			   khh,
			   0 as syl,
			   0 as zdhcl,
			   '' as hc_bq,
			   '' as hchf,
			   0 as bdl,
			   '' as bdl_bq,
			   0 as sharp,
			   0 as calma,
			   0 as beta,
			   0 as pmnl_pf,
			   '' as pmnl_pj,
			   '' as pmnl,
			   '' as pmnl_zs,
			   0 as dpsyl,
			   0 as syl_db_dp,
			   0 as ylnl_pf,
			   '' as ylnl_pj,
			   0 as fknl_pf,
			   '' as fknl_pj,
			   '' as fxbx,
			   0 as czcs,
			   0 as ztcs_gp,
			   0 as cgcgl_gp,
			   0 as cggs_gp,
			   0 as yl_gp,
			   0 as ks_gp,
			   0 as ykb,
			   dxcg,
			   zxcg,
			   cxcg,
			   0 as ztcgl_gp,
			   0 as zscgl,
			   0 as zcjy,
			   0 as ycjy,
			   '' as tzfg,
			   '' as hyph,
			   '' as ggph
			 from"||F_IDS_GET_TABLENAME('sparkCgcxx',i_khh)||"";
		F_IDS_CREATE_TEMP_TABLE(l_sqlBuf,F_IDS_GET_TABLENAME('sparkdsCdxcg',i_khh));		
		l_sqlBuf:=	"select 
			   khh,
			   0 as syl,
			   0 as zdhcl,
			   '' as hc_bq,
			   '' as hchf,
			   0 as bdl,
			   '' as bdl_bq,
			   0 as sharp,
			   0 as calma,
			   0 as beta,
			   0 as pmnl_pf,
			   '' as pmnl_pj,
			   '' as pmnl,
			   '' as pmnl_zs,
			   0 as dpsyl,
			   0 as syl_db_dp,
			   0 as ylnl_pf,
			   '' as ylnl_pj,
			   0 as fknl_pf,
			   '' as fknl_pj,
			   '' as fxbx,
			   0 as cgcgl_gp,
			   0 as cggs_gp,
			   0 as yl_gp,
			   0 as ks_gp,
			   0 as ykb,
			   0 as dxcg,
			   0 as zxcg,
			   0 as cxcg,
			   0 as czcs,
			   0 as ztcs_gp,
			   0 as ztcgl_gp,
			   0 as zscgl,
			   0 as zcjy,
			   0 as ycjy,
			   tzfg,
			   '' as hyph,
			   '' as ggph
			 from"||F_IDS_GET_TABLENAME('sparkTzfg',i_khh)||"";
		F_IDS_CREATE_TEMP_TABLE(l_sqlBuf,F_IDS_GET_TABLENAME('sparkdsTzfg',i_khh));	
		l_sqlBuf:="select 
			   khh,
			   0 as syl,
			   0 as zdhcl,
			   '' as hc_bq,
			   '' as hchf,
			   0 as bdl,
			   '' as bdl_bq,
			   0 as sharp,
			   0 as calma,
			   0 as beta,
			   0 as pmnl_pf,
			   '' as pmnl_pj,
			   '' as pmnl,
			   '' as pmnl_zs,
			   0 as dpsyl,
			   0 as syl_db_dp,
			   0 as ylnl_pf,
			   '' as ylnl_pj,
			   0 as fknl_pf,
			   '' as fknl_pj,
			   '' as fxbx,
			   0 as cgcgl_gp,
			   0 as cggs_gp,
			   0 as yl_gp,
			   0 as ks_gp,
			   0 as ykb,
			   0 as dxcg,
			   0 as zxcg,
			   0 as cxcg,
			   0 as czcs,
			   0 as ztcs_gp,
			   0 as ztcgl_gp,
			   0 as zscgl,
			   0 as zcjy,
			   0 as ycjy,
			   '' as tzfg,
			   hyph,
			   '' as ggph
			 from"||F_IDS_GET_TABLENAME('sparkHyph',i_khh)||"";
		F_IDS_CREATE_TEMP_TABLE(l_sqlBuf,F_IDS_GET_TABLENAME('sparkdsHyph',i_khh));		
		l_sqlBuf:=	 "select 
			   khh,
			   0 as syl,
			   0 as zdhcl,
			   '' as hc_bq,
			   '' as hchf,
			   0 as bdl,
			   '' as bdl_bq,
			   0 as sharp,
			   0 as calma,
			   0 as beta,
			   0 as pmnl_pf,
			   '' as pmnl_pj,
			   '' as pmnl,
			   '' as pmnl_zs,
			   0 as dpsyl,
			   0 as syl_db_dp,
			   0 as ylnl_pf,
			   '' as ylnl_pj,
			   0 as fknl_pf,
			   '' as fknl_pj,
			   '' as fxbx,
			   0 as cgcgl_gp,
			   0 as cggs_gp,
			   0 as yl_gp,
			   0 as ks_gp,
			   0 as ykb,
			   0 as dxcg,
			   0 as zxcg,
			   0 as cxcg,
			   0 as czcs,
			   0 as ztcs_gp,
			   0 as ztcgl_gp,
			   0 as zscgl,
			   0 as zcjy,
			   0 as ycjy,
			   '' as tzfg,
			   '' as hyph,
			   ggph
			 from"||F_IDS_GET_TABLENAME('sparkGgph',i_khh)||"";
		F_IDS_CREATE_TEMP_TABLE(l_sqlBuf,F_IDS_GET_TABLENAME('sparkdsGgph',i_khh));		
		l_sqlBuf:= "select 
			   khh,
			   0 as syl,
			   0 as zdhcl,
			   '' as hc_bq,
			   '' as hchf,
			   0 as bdl,
			   '' as bdl_bq,
			   0 as sharp,
			   0 as calma,
			   0 as beta,
			   0 as pmnl_pf,
			   '' as pmnl_pj,
			   '' as pmnl,
			   '' as pmnl_zs,
			   0 as dpsyl,
			   0 as syl_db_dp,
			   0 as ylnl_pf,
			   '' as ylnl_pj,
			   0 as fknl_pf,
			   '' as fknl_pj,
			   '' as fxbx,
			   0 as cgcgl_gp,
			   0 as cggs_gp,
			   0 as yl_gp,
			   0 as ks_gp,
			   0 as ykb,
			   0 as dxcg,
			   0 as zxcg,
			   0 as cxcg,
			   0 as czcs,
			   0 as ztcs_gp,
			   0 as ztcgl_gp,
			   0 as zscgl,
			   (zcjy_dx+zcjy_gp) as zcjy,
			   (ycjy_zz+ycjy_sd) as ycjy,
			   '' as tzfg,
			   '' as hyph,
			   '' as ggph
			 from"||F_IDS_GET_TABLENAME('sparkZycjy',i_khh)||"";
		F_IDS_CREATE_TEMP_TABLE(l_sqlBuf,F_IDS_GET_TABLENAME('sparkdsZycjy',i_khh));
		l_sqlBuf:=  "select 
			   khh,
			   0 as syl,
			   0 as zdhcl,
			   '' as hc_bq,
			   '' as hchf,
			   0 as bdl,
			   '' as bdl_bq,
			   0 as sharp,
			   0 as calma,
			   0 as beta,
			   0 as pmnl_pf,
			   '' as pmnl_pj,
			   '' as pmnl,
			   '' as pmnl_zs,
			   0 as dpsyl,
			   0 as syl_db_dp,
			   0 as ylnl_pf,
			   '' as ylnl_pj,
			   0 as fknl_pf,
			   '' as fknl_pj,
			   '' as fxbx,
			   0 as cgcgl_gp,
			   0 as cggs_gp,
			   0 as yl_gp,
			   0 as ks_gp,
			   0 as ykb,
			   0 as dxcg,
			   0 as zxcg,
			   0 as cxcg,
			   czcs,
			   ztcs_gp,
			   ztcgl_gp,
			   zscgl,
			   0 as zcjy,
			   0 as ycjy,
			   '' as tzfg,
			   '' as hyph,
			   '' as ggph
			 from"||F_IDS_GET_TABLENAME('sparkGpjyfx',i_khh)||"";
		F_IDS_CREATE_TEMP_TABLE(l_sqlBuf,F_IDS_GET_TABLENAME('sparkdsGpjyfx',i_khh));	
		l_sqlBuf:="SELECT * FROM F_IDS_GET_TABLENAME('sparkdsZcfx',i_khh) UNION ALL
		SELECT * FROM F_IDS_GET_TABLENAME('sparkdsCdxcg',i_khh) UNION ALL
		SELECT * FROM F_IDS_GET_TABLENAME('sparkdsGgph',i_khh) UNION ALL
		SELECT * FROM F_IDS_GET_TABLENAME('sparkdsZycjy',i_khh) UNION ALL
		SELECT * FROM F_IDS_GET_TABLENAME('sparkdsGpyk',i_khh) UNION ALL
		SELECT * FROM F_IDS_GET_TABLENAME('sparkdsHyph',i_khh) UNION ALL
		SELECT * FROM F_IDS_GET_TABLENAME('sparkdsTzfg',i_khh) UNION ALL
		SELECT * FROM F_IDS_GET_TABLENAME('sparkdsGpjyfx',i_khh)";
		F_IDS_CREATE_TEMP_TABLE(l_sqlBuf,F_IDS_GET_TABLENAME('sparkResult_1',i_khh));	
		
		l_sqlBuf:= "select 
			   khh,
			   sum(syl) as syl,
			   sum(zdhcl) as zdhcl,
			   concat_ws('',collect_set(hc_bq)) as hc_bq,
			   concat_ws('',collect_set(hchf)) as hchf,
			   sum(bdl) as bdl,
			   concat_ws('',collect_set(bdl_bq)) as bdl_bq,
			   sum(sharp) as sharp,
			   sum(calma) as calma,
			   sum(beta) as beta,
			   sum(pmnl_pf) as pmnl_pf,
			   concat_ws('',collect_set(pmnl_pj)) as pmnl_pj,
			   concat_ws('',collect_set(pmnl)) as pmnl,
			   concat_ws('',collect_set(pmnl_zs)) as pmnl_zs,
			   sum(dpsyl) as dpsyl,
			   sum(syl_db_dp) as syl_db_dp,
			   sum(ylnl_pf) as ylnl_pf,
			   concat_ws('',collect_set(ylnl_pj)) as ylnl_pj,
			   sum(fknl_pf) as fknl_pf,
			   concat_ws('',collect_set(fknl_pj)) as fknl_pj,
			   concat_ws('',collect_set(fxbx)) as fxbx,
			   sum(cgcgl_gp) as cgcgl_gp,
			   sum(cggs_gp) as cggs_gp,
			   sum(yl_gp) as yl_gp,
			   sum(ks_gp) as ks_gp,
			   sum(ykb) as ykb,
			   sum(dxcg) as dxcg,
			   sum(zxcg) as zxcg,
			   sum(cxcg) as cxcg,
			   sum(czcs) as czcs,
			   sum(ztcs_gp) as ztcs_gp,
			   sum(ztcgl_gp) as ztcgl_gp,
			   sum(zscgl) as zscgl,
			   sum(zcjy) as zcjy,
			   sum(ycjy) as ycjy,
			   concat_ws('',collect_set(tzfg)) as tzfg,
			   concat_ws('',collect_set(hyph)) as hyph,
			   concat_ws('',collect_set(ggph)) as ggph
			 from"||F_IDS_GET_TABLENAME('sparkResult_1',i_khh)||" group by khh";
		F_IDS_CREATE_TEMP_TABLE(l_sqlBuf,F_IDS_GET_TABLENAME('sparkResult_2',i_khh));
		
		l_sqlBuf:= "select 
			   khh, substring("||qmrq||",0,6) as sj,
			   cast(syl as decimal(12,6)) as syl,
			   cast(zdhcl as decimal(9,6)) as zdhcl,
			   hc_bq,
			   hchf,
			   cast(bdl as decimal(12,6)) as bdl,
			   bdl_bq,
			   cast(sharp as decimal(12,6)) as sharp,
			   cast(calma as decimal(12,6)) as calma,
			   cast(beta as decimal(12,6)) as beta,
			   cast(pmnl_pf as decimal(5,2)) as pmnl_pf,
			   pmnl_pj,
			   pmnl,
			   pmnl_zs,
			   cast(dpsyl as decimal(9,6)) as zshq_zzl,
			   cast(syl_db_dp as decimal(9,6)) as syl_db_zs,
			   cast(ylnl_pf as decimal(5,2)) as ylnl_pf,
			   ylnl_pj,
			   cast(fknl_pf as decimal(5,2)) as fknl_pf,
			   fknl_pj,
			   fxbx,
			   round(getXgnlpf(cgcgl_gp),2) as xgnl_pf,
			   getXgnlpj(cgcgl_gp) as xgnl_pj,
			   cast(cgcgl_gp as decimal(9,6)) as cgcgl_gp,
			   cast(cggs_gp as decimal(9,6)) as cggs_gp,
			   cast(yl_gp as decimal(16,2)) as yl_gp,
			   cast(ks_gp as decimal(16,2)) as ks_gp,
			   cast(ykb as decimal(9,6)) as ykb,
			   getCdxph(cast(dxcg as int), cast(zxcg as int), cast(cxcg as int)) as cdxph,
			   cast(dxcg as int) as dxcg,
			   cast(zxcg as int) as zxcg,
			   cast(cxcg as int) as cxcg,
			   cast(ztcgl_gp as decimal(9,6)) as ztcgl_gp,
			   cast(zscgl as decimal(9,6)) as zscgl,
			   getZsnlpf(zscgl, ztcgl_gp, cast(czcs as int), cast(ztcs_gp as int)) as zsnlpf,
			   getZsnlpj(zscgl, ztcgl_gp, cast(czcs as int), cast(ztcs_gp as int)) as zsnlpj,
			   getZycph(cast(zcjy as int), cast(ycjy as int)) as zycph,
			   cast(zcjy as int) as zcjy,
			   cast(ycjy as int) as ycjy,
			   getDxpph(tzfg) as  dxpfg,
			   getHyph(hyph) as hyfg,
			   getGgph(ggph) as ggfg,
			   tzfg,
			   hyph,
			   ggph
			 from"||F_IDS_GET_TABLENAME('sparkResult_2',i_khh)||"";
			 --此处用到大量udf
			F_IDS_CREATE_TEMP_TABLE(l_sqlBuf,F_IDS_GET_TABLENAME('sparkResult_3',i_khh));
			
			l_sqlBuf:="select *,
			   cast(1-rank() over(partition by sj order by zhpf desc nulls last)/count(1) over(partition by sj) as decimal(9,6)) as beat_per
			 from 
			(select
			   *,
			   cast(getZhpf(cast(ylnl_pf as double),cast(fknl_pf as double),cast(pmnl_pf as double), cast(zsnlpf as double),cast(xgnl_pf as double)) as decimal(5,2)) as zhpf,
			   getCzfgpj(cdxph, zycph, fxbx, dxpfg) as czfg
			 from"||F_IDS_GET_TABLENAME('sparkResult_3',i_khh)||")a";
			 F_IDS_CREATE_TEMP_TABLE(l_sqlBuf,F_IDS_GET_TABLENAME('sparkTzzdResult',i_khh));
	END;
	--saveData
	BEGIN
		 F_IDS_OVERWRITE_PARTITION(F_IDS_GET_TABLENAME('sparkTzzdResult', I_KHH),'apex','khfx_tznl',i_ksrq,i_khh);
		 F_IDS_OVERWRITE_PARTITION(F_IDS_GET_TABLENAME('sparkTzzdResult', I_KHH),'apex','khfx_tzfg',i_ksrq,i_khh);
		 	END;
	
RETURN;		
END P_IDS_CUST_INVESTMENT_DIAGNOSIS;