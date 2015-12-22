hive -e "use dm_recsys_model;

drop table order_product_d;

CREATE TABLE order_product_d(
order_id         STRING comment '订单id',
product_id      STRING comment '产品id'
)
COMMENT '保存每一天当中订单对应的产品'
PARTITIONED BY (dt STRING COMMENT '日期分区')
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\005'
STORED AS TEXTFILE;"