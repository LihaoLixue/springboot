#!/bin/bash
set -e
usage() {
    echo "Usage:"
    echo "  create_all_pro.sh [-u JDBC] [-n NAME] [-p PASSWORD]"
    echo "Description:"
    echo "    JDBC Inceptor's Beeline jdbc."
    echo "    NAME, the username of Inceptor."
    echo "    PASSWORD, the password of Inceptor."
    echo "Such as:"
    echo " ./create_all_pro.sh -u jdbc:hive2://127.0.0.1:10000/cust -n dingdian -p 123456"
    exit -1
}
if [ $# -ne 6 ]
then
    usage
else
    while getopts "u:n:p:" args
    do
        case $args in
            u)
                jdbc=$OPTARG
                echo $jdbc
                ;;
            n)  
            name=$OPTARG
                echo $name
                ;;
            p)  
            passwd=$OPTARG
                echo $passwd
                ;;
            ?)
                usage
                ;;
        esac
    done
fi


#执行脚本
echo -------------------------------------创建DBLINK---------------------------------------

#beeline -u ${jdbc} -n ${name} -p ${passwd} -f  ./Public/create_dblink.sql

echo -------------------------------------开始创建公共函数---------------------------------

#beeline -u ${jdbc} -n ${name} -p ${passwd} -f  ./Public/F_IDS_CREATE_TEMP_TABLE.sql
#beeline -u ${jdbc} -n ${name} -p ${passwd} -f  ./Public/F_IDS_GET_TABLENAME.sql
#beeline -u ${jdbc} -n ${name} -p ${passwd} -f  ./Public/F_IDS_OVERWRITE_PARTITION.sql
#beeline -u ${jdbc} -n ${name} -p ${passwd} -f  ./Public/P_IDS_DROP_ALL_TEMP.sql
#beeline -u ${jdbc} -n ${name} -p ${passwd} -f  ./Public/P_IDS_TRAN_INFO.sql
#beeline -u ${jdbc} -n ${name} -p ${passwd} -f  ./Public/P_IDS_IMPORT_HYPERBASE.sql
#beeline -u ${jdbc} -n ${name} -p ${passwd} -f  ./Public/P_IDS_IMPORT_HYPERBASE_NJZQ.sql

echo -------------------------------------公共函数创建完成---------------------------------

echo -------------------------------------开始创建集中交易统计------------------------------

#beeline -u ${jdbc} -n ${name} -p ${passwd} -f  ./JZJY/P_IDS_DELIVERY_ORDER_PRODUCE.sql
#beeline -u ${jdbc} -n ${name} -p ${passwd} -f  ./JZJY/P_IDS_COST_CALCUATION_EXT.sql
beeline -u ${jdbc} -n ${name} -p ${passwd} -f  ./JZJY/P_IDS_COST_CALCUATION.sql
#beeline -u ${jdbc} -n ${name} -p ${passwd} -f  ./JZJY/P_IDS_FINANCE_PRODUCT_COST.sql
beeline -u ${jdbc} -n ${name} -p ${passwd} -f  ./JZJY/P_IDS_CUST_DAILY_STAT.sql
beeline -u ${jdbc} -n ${name} -p ${passwd} -f  ./JZJY/P_IDS_INIT_CALC.sql
#beeline -u ${jdbc} -n ${name} -p ${passwd} -f  ./JZJY/P_IDS_RUNNER.sql

echo -------------------------------------集中交易统计创建完成-------------------------------


echo -------------------------------------开始创建融资融券统计-------------------------------

#beeline -u ${jdbc} -n ${name} -p ${passwd} -f  ./XY/P_IDS_XY_DELIVERY_ORDER_PRODUCE.sql
#beeline -u ${jdbc} -n ${name} -p ${passwd} -f  ./XY/P_IDS_XY_DEBT_CHECK.sql
beeline -u ${jdbc} -n ${name} -p ${passwd} -f  ./XY/P_IDS_XY_COST_CALCUTION.sql
#beeline -u ${jdbc} -n ${name} -p ${passwd} -f  ./XY/P_IDS_XY_CUST_DAILY_STAT.sql
beeline -u ${jdbc} -n ${name} -p ${passwd} -f  ./XY/P_IDS_XY_INIT_CALC.sql
#beeline -u ${jdbc} -n ${name} -p ${passwd} -f  ./XY/P_IDS_XY_RUNNER.sql

echo -------------------------------------融资融券统计创建完成-------------------------------

echo -------------------------------------开始创建个股期权统计-------------------------------

#beeline -u ${jdbc} -n ${name} -p ${passwd} -f  ./SO/P_IDS_SO_COST_CALCUTION.sql
#beeline -u ${jdbc} -n ${name} -p ${passwd} -f  ./SO/P_IDS_SO_CUST_DAILY_STAT.sql
#beeline -u ${jdbc} -n ${name} -p ${passwd} -f  ./SO/P_IDS_SO_INIT_CACL.sql
#beeline -u ${jdbc} -n ${name} -p ${passwd} -f  ./SO/P_IDS_SO_RUNNER.sql

echo -------------------------------------个股期权统计创建完成-------------------------------

echo -------------------------------------开始创建统计分析脚本-------------------------------

#beeline -u ${jdbc} -n ${name} -p ${passwd} -f  ./ANALYZE/P_IDS_DAILY_BILL.sql
beeline -u ${jdbc} -n ${name} -p ${passwd} -f  ./ANALYZE/P_IDS_DAILY_BILL_NJZQ.sql
#beeline -u ${jdbc} -n ${name} -p ${passwd} -f  ./ANALYZE/P_IDS_KHFX_YKPM.sql
#beeline -u ${jdbc} -n ${name} -p ${passwd} -f  ./ANALYZE/P_IDS_MONTHLY_BILL_STANDARD.sql

echo -------------------------------------统计分析脚本创建完成-------------------------------

echo -------------------------------------IDS处理过程创建完毕--------------------------------



