#!/bin/bash
###################################################################
#*名称 --%@NAME:定时执行任务
#*功能描述 --%@COMMENT:加载定时配置文件
#*执行周期 初始化时一次
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



crontab $BIGDATA_HOME/bin/recommendcron

