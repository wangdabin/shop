#!/bin/bash
###################################################################
#*名称 --%@NAME:测试数据表的插入
#*功能描述 --%@COMMENT:加入测试数据
####################################################################


v_pkg=recsysdmd
hive -e "
use $v_pkg;
load data local inpath '/home/sunjj5/test/hive/userTable' into table user;
load data local inpath '/home/sunjj5/test/hive/actionTable' into table action;
load data local inpath '/home/sunjj5/test/hive/platTable' into table platform;
load data local inpath '/home/sunjj5/test/hive/productTable' into table product;
load data local inpath '/home/sunjj5/test/hive/purchaseTable' into table purchase;
load data local inpath '/home/sunjj5/test/hive/shopTable' into table goods;
"


