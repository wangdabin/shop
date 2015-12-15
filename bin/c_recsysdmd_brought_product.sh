hive -e "use dm_recsys_model;

drop table brought_product;

CREATE TABLE brought_product(
user_id         STRING comment '用户id',
product_id      STRING comment '产品id'
)
COMMENT '保存用户已经购买过的产品'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\005'
STORED AS TEXTFILE;"