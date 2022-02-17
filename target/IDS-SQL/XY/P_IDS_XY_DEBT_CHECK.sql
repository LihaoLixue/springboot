!set plsqlUseSlash true
CREATE OR REPLACE PROCEDURE CUST.P_IDS_XY_DEBT_CHECK(
                            i_rq in int,
                            I_KHH in STRING
)is
/*********************************************************************************************
    *文件名称：CUST.P_IDS_XY_DEBT_CHECK
    *项目名称：IDS计算
    *文件说明：融资融券-负债明细

    创建人：王睿驹
    功能说明：融资融券-负债明细
    
    修改者            版本号            修改日期            说明
    王睿驹            v1.0.0            2019/6/14            创建
    燕居庆            v1.0.1            2019/9/25            对标java-ids 5802版本
                                                              1.增加初始化日期处理
*********************************************************************************************/
    l_sqlWhere STRING;
    l_sqlWhereLastDay STRING;
    l_sqlBuf STRING;
    l_tableName_sparkFzxxDR STRING;
    l_tableName_sparkFzxxSR STRING;
    l_tableName_sparkFzbdxxResult STRING;
    l_initDate STRING; --初始化日期
BEGIN
    --获取初始化日期
    l_initDate := 20190801;
    --SELECT F_IDS_GET_INITDATE() into l_initDate FROM system.dual;
    
    IF I_KHH IS NULL  THEN
        l_sqlWhere :=' where rq='||i_rq;
        l_sqlWhereLastDay :=' where rq='||F_GET_JYR_DATE(i_rq,-1);
    ELSE
        l_sqlWhere :=' where rq= '||i_rq||' and khh='||I_KHH;
        l_sqlWhereLastDay :=' where rq='||F_GET_JYR_DATE(i_rq,-1)||' and khh='||I_KHH;
    END IF;
    
    --sparkFzxxDR临时表创建
    BEGIN 
        l_tableName_sparkFzxxDR := F_IDS_GET_TABLENAME('xy_sparkFzxxDR', I_KHH);
        l_sqlBuf := 'select * from CUST.T_XY_FZXX_HIS '||l_sqlWhere;
    
        F_IDS_CREATE_TEMP_TABLE(l_sqlBuf,l_tableName_sparkFzxxDR);
    END;
    
    --sparkFzxxSR临时表创建
    BEGIN 
        l_tableName_sparkFzxxSR := F_IDS_GET_TABLENAME('xy_sparkFzxxSR', I_KHH);
        l_sqlBuf := 'select * from CUST.T_XY_FZXX_HIS '||l_sqlWhereLastDay;
    
        F_IDS_CREATE_TEMP_TABLE(l_sqlBuf,l_tableName_sparkFzxxSR);
    END;
    
    --sparkFzbdxxResult临时表创建
    BEGIN
        l_tableName_sparkFzbdxxResult:=F_IDS_GET_TABLENAME('xy_sparkFzbdxxResult', I_KHH);
        l_sqlBuf:="SELECT
             KHH,
             YYB,
             FSRQ,
             WTH,
             JYLB,
             JYS,
             ZQDM,
             ZQMC,
             ZQLB,
             CAST(ROUND(RZSL,0) AS DECIMAL(11,0)) AS RZSL,
             CAST(ROUND(RZJE,2) AS DECIMAL(16,2)) AS RZJE,
             CAST(ROUND(RQSL,0) AS DECIMAL(11,0)) AS RQSL,
             CAST(ROUND(RQJE,2) AS DECIMAL(16,2)) AS RQJE,
             CAST(ROUND(FZBJ,2) AS DECIMAL(16,2)) AS FZBJ,
             CAST(ROUND(HKJE,2) AS DECIMAL(16,2)) AS HKJE,
             CAST(ROUND(FZSL,0) AS DECIMAL(11,0)) AS FZSL,
             CAST(HQSL AS INT) AS HQSL,
             CAST(ROUND(RZFY,2) AS DECIMAL(16,2)) AS RZFY,
             CAST(ROUND(RQFY,2) AS DECIMAL(16,2)) AS RQFY,
             BDRQ,
             CAST(ROUND(YJLX,2) AS DECIMAL(16,2)) AS YJLX,
             CAST(ROUND(GHLX,2) AS DECIMAL(16,2)) AS GHLX,
             CAST(ROUND(ZXSZ,2) AS DECIMAL(16,2)) AS ZXSZ,
             CAST(ROUND(YSJE_RQMC,2) AS DECIMAL(16,2)) AS YSJE_RQMC,
             CAST(ROUND(XZRQFZ,2) AS DECIMAL(16,2)) AS XZRQFZ,
             CAST(ROUND(XZRQYJLX,2) AS DECIMAL(16,2)) AS XZRQYJLX,
             CAST(ROUND(XZRZYJLX,2) AS DECIMAL(16,2)) AS XZRZYJLX,
             CAST(ROUND(FDYK,2) AS DECIMAL(16,2)) AS FDYK,
             CAST(ROUND(CASE
               WHEN (RQ = FSRQ  OR RQ = " || l_initDate || " ) AND JYLB = '64' THEN
                FDYK  -- 对于首日融券卖出，当日盈亏就等于浮动盈亏
                               WHEN JYLB = '64' THEN
                - (XZRQFZ + XZRQYJLX) -- 对于融券卖出的盈亏只要市值不断往下跌，则表示盈利越多，也就是负的越多，挣的越多，所以前面加个反向符号'-'
                               WHEN JYLB = '61' THEN
                -XZRZYJLX  -- 对于融资买入而言，每天多出来的利息就是亏损,所以前面加个反向符号'-'
                             END,2) AS DECIMAL(16,2)) AS DRYK,
             CAST(ROUND(XZRQHKJE + XZRZHKJE,2) AS DECIMAL(16,2)) AS XZHKJE,
             CAST(ROUND(XZRQHKJE,2) AS DECIMAL(16,2)) AS XZRQHKJE,
             CAST(ROUND(XZRZHKJE,2) AS DECIMAL(16,2)) AS XZRZHKJE,
             FZZT,
             CAST(ROUND(XZRQGHLX + XZRZGHLX,2) AS DECIMAL(16,2)) AS XZGHLX,
             CAST(ROUND(XZRZGHLX,2) AS DECIMAL(16,2)) AS XZRZGHLX,
             CAST(ROUND(XZRQGHLX,2) AS DECIMAL(16,2)) AS XZRQGHLX,
             RQ
        FROM (SELECT A1.RQ,
                     A1.KHH,
                     A1.YYB,
                     A1.FSRQ,
                     A1.BDRQ,
                     A1.WTH,
                     A1.JYLB,
                     A1.JYS,
                     A1.ZQDM,
                     A1.ZQMC,
                     A1.ZQLB,
                     A1.FZZT,
                     SUM(A1.RZSL) AS RZSL,
                     SUM(A1.RZJE) AS RZJE,
                     SUM(A1.RQSL) AS RQSL,
                     SUM(A1.RQJE) AS RQJE,
                     SUM(A1.FZBJ) AS FZBJ,
                     SUM(A1.HKJE) AS HKJE,
                     SUM(A1.FZSL) AS FZSL,
                     SUM(A1.HQSL) AS HQSL,
                     SUM(A1.RZFY) AS RZFY,
                     SUM(A1.RQFY) AS RQFY,
                     SUM(A1.YJLX) AS YJLX,
                     SUM(A1.GHLX) AS GHLX,
                     SUM(A1.ZXSZ) AS ZXSZ,
                     SUM(CASE
                           WHEN A1.JYLB IN ('64') AND A1.FZZT <> 3 THEN
                            (A1.RQJE - A1.RQFY)
                           ELSE
                            0
                         END) AS YSJE_RQMC,   -- 卖出后所得也就是后续计算浮动盈亏的基准值
                                    SUM(CASE
                           WHEN A1.JYLB IN ('64') AND A1.FZZT <> 3 THEN
                            (A1.ZXSZ + A1.RQFY) -
                            (nvl(A2.ZXSZ, 0) + nvl(A2.RQFY, 0))
                           ELSE
                            0
                         END) AS XZRQFZ,
                     SUM(CASE
                           WHEN A1.JYLB IN ('64') AND A1.FZZT <> 3 THEN
                            (A1.YJLX - nvl(A2.YJLX, 0))
                           ELSE
                            0
                         END) AS XZRQYJLX,
                     SUM(CASE
                           WHEN A1.JYLB IN ('61') AND A1.FZZT <> 3 THEN
                            (A1.YJLX - nvl(A2.YJLX, 0))
                           ELSE
                            0
                         END) AS XZRZYJLX,
                     SUM(CASE
                           WHEN A1.JYLB IN ('64') AND A1.FZZT <> 3 THEN
                            (A1.RQJE - A1.RQFY) - (A1.ZXSZ + A1.RQFY + A1.YJLX)
                           ELSE
                            0
                         END) AS FDYK,
                     SUM(CASE
                           WHEN A1.JYLB IN ('64') /*AND A1.YJLX<>0*/
                            THEN
                            (A1.HKJE - nvl(A2.HKJE, 0))
                           ELSE
                            0
                         END) AS XZRQHKJE,
                     SUM(CASE
                           WHEN A1.JYLB IN ('61') /*AND A1.YJLX<>0*/
                            THEN
                            (A1.HKJE - nvl(A2.HKJE, 0))
                           ELSE
                            0
                         END) AS XZRZHKJE,
                     SUM(CASE
                           WHEN A1.JYLB IN ('64') AND A1.YJLX <> 0 THEN
                            (A1.GHLX - nvl(A2.GHLX, 0))
                           ELSE
                            0
                         END) AS XZRQGHLX,
                     SUM(CASE
                           WHEN A1.JYLB IN ('61') AND A1.YJLX <> 0 THEN
                            (A1.GHLX - nvl(A2.GHLX, 0))
                           ELSE
                            0
                         END) AS XZRZGHLX
                FROM (SELECT T.WTH,
                             T.RQ,
                             T.KHH,
                             T.KHXM,
                             T.YYB,
                             T.FSRQ,
                             T.JYLB,
                             T.JYS,
                             T.GDH,
                             T.ZQDM,
                             T.ZQMC,
                             T.ZQLB,
                             T.RZSL,
                             T.RZJE,
                             T.RQSL,
                             T.RQJE,
                             T.FZBJ,
                             T.HKJE,
                             T.FZSL,
                             T.HQSL,
                             T.RZFY,
                             T.RQFY,
                             T.BDRQ,
                             T.YJLX,
                             T.GHLX,
                             T.BZ,
                             T.DQRQ,
                             T.FZZT,
                             T.ZXSZ
                        FROM "||l_tableName_sparkFzxxDR||" T
                       WHERE T.JYLB IN ('61', '64')) A1
                LEFT JOIN (SELECT T.RQ,
                                 T.WTH,
                                 T.KHH,
                                 T.FSRQ,
                                 T.JYLB,
                                 T.JYS,
                                 T.GDH,
                                 T.ZQDM,
                                 T.ZQMC,
                                 T.RQSL,
                                 T.RQJE,
                                 T.RQFY,
                                 T.ZXSZ,
                                 T.YJLX,
                                 T.GHLX,
                                 T.HKJE,
                                 T.FZZT
                            FROM "||l_tableName_sparkFzxxSR||" T
                           WHERE T.JYLB IN ('61', '64')
                             AND FZZT <> 3) A2
                  ON A1.KHH = A2.KHH
                 AND A1.GDH = A2.GDH
                 AND A1.JYS = A2.JYS
                 AND A1.ZQDM = A2.ZQDM
                 AND A1.WTH = A2.WTH
               GROUP BY A1.RQ,
                        A1.KHH,
                        A1.YYB,
                        A1.FSRQ,
                        A1.BDRQ,
                        A1.WTH,
                        A1.JYLB,
                        A1.JYS,
                        A1.ZQDM,
                        A1.ZQMC,
                        A1.ZQLB,
                        A1.FZZT) D";
                        
        F_IDS_CREATE_TEMP_TABLE(l_sqlBuf,l_tableName_sparkFzbdxxResult);
    END;
    
    BEGIN
        F_IDS_OVERWRITE_PARTITION(l_tableName_sparkFzbdxxResult,"cust","T_XY_FZXXBDMX_HIS",i_rq,i_khh);
    END;
END;
/