#!/bin/bash
###################################################################
#*名称 --%@NAME:将对应als的分析结果导入到hive model_cf表中
#*功能描述 --%@COMMENT:删除旧的model_cf中的数据,导入新的数据
####################################################################


v_pkg=recsysdmd


#1.首先删除对应的model_cf表
hive -e "
use $v_pkg;
drop table model_cf;

CREATE TABLE model_cf(
id STRING comment '推荐标识',
user_id         STRING comment '用户标识',
product_id      STRING comment '产品标识',
score           DOUBLE comment '推荐程度',
create_time     BIGINT comment '更新时间'
)
COMMENT '保存用户对所有物品的评分'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;


load data inpath 'model_cf' into table model_cf;
"


