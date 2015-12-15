#!/bin/bash
###################################################################
#*名称 --%@NAME:测试数据表的创建
#*功能描述 --%@COMMENT:加入测试数据
#*目标表 --%@TO:recsysdmd.users

####################################################################

v_pkg=recsysdmd

hive -e "drop database $v_pkg cascade";
hive -e "CREATE DATABASE IF NOT EXISTS $v_pkg";

user_sql="
create table  user
(
  id         string  comment    '用户标识',
  createTime      string  comment    '创建时间'
)
comment '用户表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';
"
platform_sql="
create table  platform
(
  id         string  comment    '用户标识',
  type int comment '平台类别',
  name  string    comment    '平台描述'
)
comment '平台表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';
"

products_sql="
create table  product
(

  id         string  comment    '类型id',
  name      string  comment    '产品类型名称',
  business string comment   '业务部门',
  type   int comment '产品类别',
  materialcode string comment '物料号',
  specification string comment '产品规格',
  createTime      bigint  comment    '产品时间'
)
comment '产品表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';
"

goods_sql="
create table  goods
(

  id         string  comment    '商品ID',
  productid      string  comment    '产品id',
  platformid string comment '平台标识',
  code string comment '商品号',
  saletype string,
  price         string  comment    '商品价格',
  marketable   string,
  isgift     string ,
  istest     string ,
  createTime  bigint  comment    '创建时间'
)
comment '商品'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';
"


create table stock_info(
    product_code string,
     business_unit string,
     sales_number int)
     ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';


actions_sql="
create table action
(

  id         string  comment    '行为唯一标识',
  userid      string  comment    '用户标识',
  type         int  comment    '行为类型0：购买；1：点击；2：收藏；3：分享；4：评论；5：放入购物车；6：立即下单',
  itemid      string  comment    '如果行为类型是购买，为订单标识,否则为商品标识（关联Goods表中ID字段）',
  platformid         string  comment    '关联Platform表中ID字段',
  createTime  bigint  comment    '行为时间'
)
PARTITIONED BY (dt STRING COMMENT '日期分区')
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';
"
purchases_sql="
create table purchase
(

  id         string  comment    '购品标识',
  goodsid      string  comment    '关联Goods表中ID字段',
  orderid         string  comment    '关联Action表ItemID字段',
  goodsnumber  int  comment    '商品个数'
)
comment '购品表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';
"


echo "开始创建hive数据库"

hive -e "
use $v_pkg;
$user_sql
$platform_sql
$products_sql
$goods_sql
$actions_sql
$purchases_sql
"
