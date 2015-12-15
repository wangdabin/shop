#!/bin/bash
###################################################################
#*名称 --%@NAME:用户表数据迁移
#*功能描述 --%@COMMENT:提取用户数据
#*来源表 --%@FROM:piwikods.shop_piwik_log_visit
#*来源表 --%@FROM:shopods.ordermains
#*目标表 --%@TO:recsysdmd.users
####################################################################
#Hive调用业务逻辑脚本，进行数据加工作业

v_pkg='recsysdmd'
v_table='users'
v_logfile='logs/${v_table}.log'

v_sql="
select userid,goodid,count(*) grade,min(createtime)
from action
where action.type = "1"
group by action.userid,action.goodid;
"

hive -e "
use $v_pkg;
$v_sql
" 2>&1 |tee $v_logfile >>/dev/null

#获取sql执行结果信息
  v_result=`cat $v_logfile | grep -s "FAILED" | awk -F ":" '{print $1}'` >>/dev/null



#判断数据是否导出成功
if [ "$v_result" != "FAILED" ]; then
     v_retcode='SUCCESS'
     v_retinfo='结束'
  else
     v_retcode='FAIL'
     v_retinfo=`cat $v_logfile | grep -s "FAILED"` >>/dev/null
 fi
