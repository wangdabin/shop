#!/bin/bash
###################################################################
#*名称 --%@NAME:产品推荐ALS
#*功能描述 --%@COMMENT:分析数据,得到对应的推荐结果,保存到对应的hive表中
#*来源表 --%@FROM_TABLE:dm_recsys_model.score_weight_sum
#*目标表  --%@TARGET_TABLE:dm_recsys_model.model_cf
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
PROGRAM_CLASS="mlearn.ProductLensALS"
JAR_PATH="${BIGDATA_HOME}/lib/sbtspark_2.10-1.0.jar"
MYSQL_JAR="${BIGDATA_HOME}/lib/mysql-connector-java-5.1.7-bin.jar"

#分析
echo "开始分析数据"
spark-submit --master $SPARK_MASTER_URL --executor-memory 5G --total-executor-cores 24 --class $PROGRAM_CLASS  $JAR_PATH


#将hdfs中的数据加载到对应的model_cf表中,不保留原有的数据
hive -e "
use dm_recsys_model;
LOAD DATA INPATH 'model_cf/als' OVERWRITE INTO TABLE model_cf;
"