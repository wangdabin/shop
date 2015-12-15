#!/bin/bash
###################################################################
#*名称 --%@NAME:用户产品评分
#*功能描述 --%@COMMENT:统计用户的评分数据
#*执行周期 用户指定
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
  echo "Usage: <StartTime>  eg: 20150720"
  echo "Usage: <StartTime> <EndTime>  eg: 20150720 20150722"
  exit 1
fi

#执行一天的程序
if [ $# -eq 1 ]; then
startTime=$1
 $BIGDATA_HOME/bin/p_recsysdmd_score_day.sh $startTime > $BIGDATA_HOME/logs/score_$startTime.log 2>&1 &
fi
#执行一段时间的程序
if [ $# -eq 2 ]; then
startTime=$1
endTime=$2
$BIGDATA_HOME/bin/p_recsysdmd_score_days.sh $startTime $endTime > $BIGDATA_HOME/logs/score_${startTime}_${endTime}.log  2>&1 &
fi
