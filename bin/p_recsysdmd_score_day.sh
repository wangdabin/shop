#!/bin/bash
###################################################################
#*名称 --%@NAME:用户产品评分
#*功能描述 --%@COMMENT:统计用户每天的评分数据
#*执行周期 每天
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
  echo "Usage: <Time>  eg: 19911031"
  exit 1
fi

startTime=$1

v_pkg=recsysdmd

v_sql="
INSERT overwrite TABLE dm_recsys_model.user_product_score_d partition(dt='$startTime')
SELECT mid_action.user_id,
       mid_action.product_id,
       sum(mid_action.score)
FROM
(
  SELECT action.userid user_id,
          goods.productid product_id,
          5 score
   FROM
     (SELECT *
      FROM action
      WHERE action.TYPE = 0
        AND action.dt = ${startTime} AND action.userid is not null) action
   LEFT OUTER JOIN purchase ON action.itemid = purchase.orderid
   LEFT OUTER JOIN goods ON purchase.goodsid = goods.id
   WHERE  goods.isgift = 0 AND goods.istest = 0
   UNION ALL
   SELECT action.userid user_id,
          goods.productid product_id,
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
        AND action.dt = ${startTime}
        AND action.userid is not null
   ) action
   LEFT OUTER JOIN goods ON action.itemid = goods.id
   WHERE  goods.isgift = 0 AND goods.istest = 0
) mid_action
GROUP BY mid_action.user_id,mid_action.product_id
"

#Hive调用业务逻辑脚本，进行数据加工作业
hive -e "
use $v_pkg;
$v_sql
"




