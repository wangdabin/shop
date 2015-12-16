#!/bin/bash
###################################################################
#*名称 --%@NAME:买了又买
#*功能描述 --%@COMMENT:分析数据,得到对应的买了又买的商品
#*来源表 --%@FROM_TABLE:purchase
#*目标表  --%@TARGET_TABLE:mysql: bought_also_bought
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


#SPARK 参数设置
SPARK_MASTER_URL="spark://cidadm01:7077"
PROGRAM_CLASS="popularization.PopularizationRecom"
JAR_PATH="${BIGDATA_HOME}/lib/sbtspark_2.10-1.0.jar"
MYSQL_JAR="${BIGDATA_HOME}/lib/mysql-connector-java-5.1.7-bin.jar"

#分析
echo "开始分析数据"
spark-submit --master $SPARK_MASTER_URL --executor-memory 5G --total-executor-cores 24 --class $PROGRAM_CLASS  $JAR_PATH
