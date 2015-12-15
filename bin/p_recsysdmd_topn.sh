#!/bin/bash
###################################################################
#*名称 --%@NAME:用户表数据迁移
#*功能描述 --%@COMMENT:分析数据,并得到对应的统计数据,导入到mysql中
#*执行周期 手工
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
  echo "Usage: <startTime> <stopTime>  eg: 20151120 20151123"
  exit 1
fi

startTime=$1
endTime=$2

v_pkg=recsysdmd
v_logfile=${BIGDATA_HOME}/logs/topn_${startTime}.log
v_sql="
INSERT overwrite TABLE dm_recsys_model.top_n
SELECT product_id,
       sum(score)
FROM
(
SELECT    goods.productid product_id,
          5 score
FROM
(SELECT *
 FROM action
 WHERE action.TYPE = 0
 AND action.dt >= ${startTime}
 AND action.dt <= ${endTime}
 AND action.userid is not null
) action
LEFT OUTER JOIN purchase ON action.itemid = purchase.orderid
LEFT OUTER JOIN goods ON purchase.goodsid = goods.id
WHERE  goods.isgift = 0 AND goods.istest = 0
UNION ALL
SELECT    goods.productid product_id,
          CASE
          WHEN action.TYPE = 1 THEN 1
          WHEN action.TYPE = 2 THEN 2
          WHEN action.TYPE = 3 THEN 3
          WHEN action.TYPE = 5 THEN 4
          WHEN action.TYPE = 6 THEN 5
          ELSE 0
          END AS score
FROM
(SELECT *
 FROM action
 WHERE action.TYPE <> 0
  AND action.TYPE <> 4
  AND action.dt >= ${startTime}
  AND action.dt <= ${endTime}
  AND action.userid is not null
) action
LEFT OUTER JOIN goods ON action.itemid = goods.id
   WHERE  goods.isgift = 0 AND goods.istest = 0
) mid_action
GROUP BY mid_action.product_id
"
echo $v_sql

#Hive调用业务逻辑脚本，进行数据加工作业
hive -e "
use $v_pkg;
$v_sql
"
