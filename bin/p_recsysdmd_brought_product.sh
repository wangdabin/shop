#!/bin/bash
###################################################################
#*名称 --%@NAME:用户已经购买的产品
#*功能描述 --%@COMMENT:统计用户每天购买的产品
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

v_pkg=recsysdmd

v_sql="
INSERT overwrite TABLE dm_recsys_model.brought_product
   SELECT action.userid,goods.productid
   FROM
   (SELECT *
      FROM action
      WHERE action.TYPE = 0
        AND action.userid is not null) action
   LEFT OUTER JOIN purchase ON action.itemid = purchase.orderid
   LEFT OUTER JOIN goods ON purchase.goodsid = goods.id
   WHERE  goods.isgift = 0 AND goods.istest = 0
   GROUP BY   action.userid,goods.productid
"

#Hive调用业务逻辑脚本，进行数据加工作业
hive -e "
use $v_pkg;
$v_sql
"




