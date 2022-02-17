!set plsqlUseSlash true
CREATE OR REPLACE PROCEDURE cust.p_ids_khfx_ykpm (
    I_RQ INT,
    I_KHH STRING
)
IS
    l_j1n INT;
    l_j1y INT;
    l_sqlBuf STRING;
    l_tableName STRING;
BEGIN
    --获取统计日期
    SELECT f_get_date(I_RQ, 6) INTO l_j1n FROM system.dual;
    SELECT f_get_date(I_RQ, 5) INTO l_j1y FROM system.dual;
    
    --创建收益率统计临时表
    BEGIN
        l_tableName := F_IDS_GET_TABLENAME('sparkCalcSyl', I_KHH);

        EXECUTE IMMEDIATE "DROP TABLE IF EXISTS " || l_tableName;
        EXECUTE IMMEDIATE "create table " || l_tableName  || "(khh string,zzcJoin string) row format delimited fields terminated by '\:' stored as textfile";
    END;
    --汇总写入收益率
    BEGIN
        l_sqlBuf := "INSERT INTO " || l_tableName ||
                    " SELECT " ||
                    " f_ids_calc_syl(ROW) " ||
                    " FROM  " ||
                    " (SELECT " ||
                    " KHH " ||
                    " , GROUPROW(KHH, ZZC, ZCJLR, RQ) AS ROW " ||
                    " FROM " ||
                    " CUST.T_STAT_ZD_R " ||
                    " WHERE RQ BETWEEN " || L_J1N || " AND " || I_RQ ||
                    " GROUP BY " ||
                    " KHH) A";
        EXECUTE IMMEDIATE l_sqlBuf;
    END;
    
    --创建收益率排名临时表
    BEGIN
        l_tableName := F_IDS_GET_TABLENAME("sparkKhfxYkpm", I_KHH);

        EXECUTE IMMEDIATE "DROP TABLE " || l_tableName;
        EXECUTE IMMEDIATE "CREATE TABLE IF NOT EXISTS " || l_tableName ||
        " AS " ||
        "SELECT * FROM cust.t_stat_khfx_ykpm WHERE 1=2";
    END;
    
    --写入当日盈亏/盈亏率/排名
    BEGIN
        l_sqlBuf := "INSERT INTO " || l_tableName ||
                    " (KHH, YK, YKL, YKL_PM) " ||
                    " SELECT KHH, " ||
                    " YK, " ||
                    " YKL, " ||
                    " percent_rank() over (order by YKL ASC ) as YKL_PM " ||
                    " FROM " ||
                    " cust.T_stat_zd_r WHERE rq = " || I_RQ;

        EXECUTE IMMEDIATE l_sqlBuf;
    END;
    
    --写入近一年盈亏
    BEGIN
        l_sqlBuf := "INSERT INTO  " || l_tableName ||
                    "(KHH, YK_J1N) " ||
                    " SELECT KHH, " ||
                    "    sum(yk) AS YK_J1N " ||
                    " FROM " || 
                    " cust.T_stat_zd_r WHERE rq BETWEEN " || l_j1n || " AND " || I_RQ || " GROUP BY khh";
        EXECUTE IMMEDIATE l_sqlBuf;
    END;
    
    --写入近一月盈亏
    BEGIN
        l_sqlBuf := "INSERT INTO  " || l_tableName ||
                    "(KHH, YK_J1Y) " ||
                    " SELECT KHH, " ||
                    "    sum(yk) AS YK_J1Y " ||
                    " FROM " || 
                    " cust.T_stat_zd_r WHERE rq BETWEEN " || l_j1y || " AND " || I_RQ || " GROUP BY khh";
        EXECUTE IMMEDIATE l_sqlBuf;
    END;

    set_env('character.literal.as.string', true);
    
    --写入近一年盈亏率/排名
    BEGIN
        l_sqlBuf := "INSERT INTO  " || l_tableName ||
                    "(KHH, YKL_J1N, YKL_PM_J1N) " ||
                    " SELECT KHH, " ||
                    "      YKL AS YKL_J1N, " ||
                    "      percent_rank() over (order by YKL ASC ) as YKL_PM_J1N " ||
                    " from ( " ||
                    "    select khh,f_ids_get_syl(zzcjoin," || l_j1n || ", " || I_RQ || ") AS YKL FROM "  
                    || F_IDS_GET_TABLENAME('sparkCalcSyl', I_KHH) || " ORDER BY YKL ASC) T ";

        EXECUTE IMMEDIATE l_sqlBuf;
    END;
    
    --写入近一月盈亏率/排名
    BEGIN
        l_sqlBuf := "INSERT INTO  " || l_tableName ||
                    "(KHH, YKL_J1Y, YKL_PM_J1Y) " ||
                    " SELECT KHH, " ||
                    "      YKL AS YKL_J1Y, " ||
                    "      percent_rank() over (order by YKL ASC ) as YKL_PM_J1Y " ||
                    " from ( " ||
                    "    select khh,f_ids_get_syl(zzcjoin," || l_j1y || ", " || I_RQ || ") AS YKL FROM " ||  
                    F_IDS_GET_TABLENAME('sparkCalcSyl', I_KHH) || " ORDER BY YKL ASC) T ";

        EXECUTE IMMEDIATE l_sqlBuf;
    END;
    
    --数据汇总写入
    BEGIN
	    l_sqlBuf := "DELETE FROM cust.t_stat_khfx_ykpm partition(YF = "|| substr(I_RQ,1,6) || ") WHERE rq = " || I_RQ;
        EXECUTE IMMEDIATE l_sqlBuf;
        
        set_env('hive.exec.dynamic.partition', true);
        
        
        l_sqlBuf := "INSERT INTO cust.t_stat_khfx_ykpm " ||
                    " PARTITION (YF) " ||
                    "SELECT  A.KHH, " ||
                            I_RQ || " AS RQ, " ||
                    "        sum(YK) AS YK, " ||
                    "        sum(YKL) AS YKL, " ||
                    "        round(sum(YKL_PM),4) AS YKL_PM, " ||
                    "        sum(YK_J1N) AS YK_J1N, " ||
                    "        sum(YKL_J1N) AS YKL_J1N, " ||
                    "        round(sum(YKL_PM_J1N),4) AS YKL_PM_J1N, " ||
                    "        sum(YK_J1Y) AS YK_J1Y, " ||
                    "        sum(YKL_J1Y) AS YKL_J1Y, " ||
                    "        round(sum(YKL_PM_J1Y),4) AS YKL_PM_J1Y, " ||
                            substr(I_RQ,1,6) || " AS YF " ||
                    "    FROM " || l_tableName || " A GROUP BY KHH";
        
        EXECUTE IMMEDIATE l_sqlBuf;
    END;
    
    --数据写入HyperBase
    BEGIN
        INSERT INTO APEX.KHFX_YKPM
            SELECT /*+USE_BULKLOAD*/
                concat_ws('-',reverse(KHH), rq) AS rowkey,
                A.KHH,
                RQ,
                YK,
                YKL,
                YKL_PM,
                YK_J1N,
                YKL_J1N,
                YKL_PM_J1N,
                YK_J1Y,
                YKL_J1Y,
                YKL_PM_J1Y
            FROM cust.t_stat_khfx_ykpm A WHERE YF = substr(I_RQ,1,6) AND RQ = I_RQ ORDER BY rowkey;
    END;
END;
/