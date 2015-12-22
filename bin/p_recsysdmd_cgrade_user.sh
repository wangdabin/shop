#!/bin/bash
###################################################################
#*名称 --%@NAME:综合推荐
#*功能描述 --%@COMMENT:分析数据,得到对应的综合推荐结果,存放到mysql中
#*来源表 --%@SOURCE_TABLE:model_cf
#*目标表 --%@TARGET_TABLE:cf_mf_user
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


# if no args specified, show usage
if [ $# -lt 1 ]; then
  echo "Usage: <recommend nums> 推荐个数 eg: 20"
  exit 1
fi
recommend_nums=$1


JAR_PATH="${BIGDATA_HOME}/lib/sbtSpark-assembly-1.0.jar"


#MYSQL_JAR="${BIGDATA_HOME}/lib/mysql-connector-java-5.1.7-bin.jar"

SPARK_MASTER_URL="spark://cidadm01:7077"
SPARK_HOME="/usr/lib/spark/spark-1.4.1-bin-hadoop2.4/bin"

#数据库信息
HOSTNAME="10.250.100.15"
PORT="3306"
USERNAME="recommend"
PASSWORD="recommend1234"
DBNAME="recommend"
TABLENAME="cf_mf_user"


#创建表
echo "检查表是否存在"
create_table_sql="create table IF NOT EXISTS ${TABLENAME} (
id char(36) primary key COMMENT '推荐标识',
lenovo_id char(36) COMMENT '联想用户',
le_id  char(36) COMMENT '设备用户',
platform_type int COMMENT '平台类型编码',
goods_codes varchar(255) COMMENT '推品商品编号',
create_time bigint COMMENT '创建时间'
);
"
mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e"${create_table_sql}"


#分析
#echo "开始分析数据"
$SPARK_HOME/spark-submit --master $SPARK_MASTER_URL --executor-memory 5G --total-executor-cores 24 --class sql.CompositeGrade  $JAR_PATH  $recommend_nums