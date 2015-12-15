#!/bin/bash
###################################################################
#*名称 --%@NAME:用户产品评分
#*功能描述 --%@COMMENT:统计用户一段时间的评分数据
#*执行周期 一段时间
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
  echo "Usage: <StartTime> <EndTime>  eg: 20150720 20150722"
  exit 1
fi


startTime=$1
endTime=$2

beg_s=`date -d "$startTime" +%s`
end_s=`date -d "$endTime" +%s`

echo "开始执行程序...."
while [ "$beg_s" -le "$end_s" ];do
     day=`date -d @$beg_s +"%Y%m%d"`;
     echo "执行" + $day + "数据..."
     $BIGDATA_HOME/bin/p_recsysdmd_score_day.sh $day
     beg_s=$((beg_s+86400));
done
echo "执行完毕"


