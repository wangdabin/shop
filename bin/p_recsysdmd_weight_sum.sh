#!/bin/bash
###################################################################
#*名称 --%@NAME:用户产品评分加权计算
#*功能描述 --%@COMMENT:统计一段时间内用户评分数据的加权值
#*执行周期 定时执行
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
if [ $# -lt 2 ]; then
  echo "Usage: <Time> <Limit>  eg: 20151124 90"
  exit 1
fi

startTime=$1
days=$2
filter_days=30
times=1
v_pkg=dm_recsys_model
v_logfile=${BIGDATA_HOME}/logs/weight_sum_${startTime}.log
v_jar=${BIGDATA_HOME}/lib/sbtspark_2.10-1.0.jar
v_sql="
INSERT overwrite TABLE score_weight_sum
select user_id,product_id,round(sum(weight_score)/sum(weight),2) score
from
(
 select user_id,
       product_id,
       score * w_s_s($startTime,dt,$days) weight_score,
       w_s_s($startTime,dt,$days) weight
 from
 (
 select mid3.user_id user_id,mid3.product_id product_id,mid3.score score,mid3.dt dt
 from
 (select user_id
 from(
 select user_id,count(*) login_times
 from(
 select dt,user_id
 from user_product_score_d
 where dt <= $startTime and dt > date_add($startTime,-$filter_days)
 group by dt,user_id
 ) mid
 group by mid.user_id
 )  mid1
 where mid1.login_times >$times) mid2
 left outer join
 (select *
   from user_product_score_d
   where dt <= $startTime and dt > date_add($startTime,-$days)
 ) mid3
 on mid2.user_id = mid3.user_id
 ) mid4
)  mid5
group by mid5.user_id,mid5.product_id
"
echo $v_sql

#Hive调用业务逻辑脚本，进行数据加工作业
hive -e "
use $v_pkg;
add jar $v_jar;
create temporary function w_s_s as 'com.lenovo.hive.udf.WeightSumScore';
create temporary function date_add as 'com.lenovo.hive.udf.DateAddDay';
$v_sql
"
#> $v_logfile 2>&1 &
