#!/bin/bash
###################################################################
#*名称 --%@NAME:程序流程控制
#*功能描述 --%@COMMENT:将每天的数据进行处理
#*执行周期 day
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

#得到当前日期
current_day=`date "+%Y%m%d" -d'-1 day'`
echo "开始分析${current_day}的数据..."
#计算topN的开始日期
start_day=`date "+%Y%m%d" -d'-91 day'`

#要分析的天数
date_interval=90

#混合推荐结果条数
total_results=20


#统计每天的用户评分
$BIGDATA_HOME/bin/p_recsysdmd_score_day.sh $current_day
if [ $? -ne 0 ];then
echo "执行失败"
exit
fi
#综合90天内的用户综合评分
$BIGDATA_HOME/bin/p_recsysdmd_weight_sum.sh $current_day $date_interval
if [ $? -ne 0 ];then
echo "执行失败"
exit
fi
#统计用户已经买过的产品
$BIGDATA_HOME/bin/p_recsysdmd_brought_product.sh
if [ $? -ne 0 ];then
echo "执行失败"
exit
fi
#ALS推测用户评分数据
$BIGDATA_HOME/bin/p_recsysdmd_als_user.sh
if [ $? -ne 0 ];then
echo "执行失败"
exit
fi
#混合推荐模型
$BIGDATA_HOME/bin/p_recsysdmd_cgrade_user.sh $total_results
if [ $? -ne 0 ];then
echo "执行失败"
exit
fi
#topn统计
$BIGDATA_HOME/bin/p_recsysdmd_topn.sh $start_day $current_day
if [ $? -ne 0 ];then
echo "执行失败"
exit
fi
#topn to mysql
$BIGDATA_HOME/bin/p_recsysdmd_transform_topn.sh
if [ $? -ne 0 ];then
echo "执行失败"
exit
fi
#goods to mysql
$BIGDATA_HOME/bin/p_recsysdmd_transform_goods.sh
if [ $? -ne 0 ];then
echo "执行失败"
exit
fi



echo "执行成功"