#!/bin/bash
###################################################################
#*名称 --%@NAME:订单产品信息统计
#*功能描述 --%@COMMENT:统计每天订单中的产品信息
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
INSERT overwrite TABLE dm_recsys_model.order_product_d partition(dt='$startTime')
SELECT orderid,
       productid
FROM
  (SELECT *
   FROM action
   WHERE dt = ${startTime}
     AND TYPE = 0) mid_action
JOIN purchase ON mid_action.itemid = purchase.orderid
JOIN goods ON purchase.goodsid = goods.id
GROUP BY orderid,
         productid
"

#Hive调用业务逻辑脚本，进行数据加工作业
hive -e "
use $v_pkg;
$v_sql
"




