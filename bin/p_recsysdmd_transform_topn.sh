#!/bin/bash
###################################################################
#*名称 --%@NAME:综合推荐
#*功能描述 --%@COMMENT:分析数据,得到对应的综合推荐结果,存放到mysql中
#*来源表 --%@SOURCE_TABLE:dm_recsys_model.top_n
#*目标表 --%@TARGET_TABLE:recommend.top_n
#*执行周期 人工调用
####################################################################
THIS="$0"

while [ -h "$THIS" ]; do
ls=`ls -ld "$THIS"`
link=`expr "$ls" : '.*-> \(.*\)$'`
if expr "$link" : '.*/.*' > /dev/null; then
THIS="$link"
else
THIS=`dirname "$THIS"`/"$link"
fi
done
THIS_DIR=`dirname "$THIS"`
BIGDATA_HOME=`cd "$THIS_DIR/.." ; pwd`


JAR_PATH="${BIGDATA_HOME}/lib/sbtSpark-assembly-1.0.jar"


#MYSQL_JAR="${BIGDATA_HOME}/lib/mysql-connector-java-5.1.7-bin.jar"

SPARK_HOME="/usr/lib/spark/spark-1.4.1-bin-hadoop2.4/bin"
SPARK_MASTER_URL="spark://cidadm01:7077"
#数据库信息
HOSTNAME="10.250.100.15"
PORT="3306"
USERNAME="recommend"
PASSWORD="recommend1234"
DBNAME="recommend"
TABLENAME="top_n"


#创建表
echo "检查表是否存在"
create_table_sql="create table IF NOT EXISTS ${TABLENAME} (
id char(36) primary key COMMENT '推荐标识',
platform_type int COMMENT '平台类型编号',
product_type  int COMMENT '产品类型编号',
goods_code int COMMENT '商品号码',
score double COMMENT '热度评分',
create_time bigint COMMENT '创建时间'
);
"
mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e"${create_table_sql}"

#分析
#echo "开始分析数据"
$SPARK_HOME/spark-submit --master $SPARK_MASTER_URL --executor-memory 5G --total-executor-cores 24 --class sql.TopNExportMysql  $JAR_PATH