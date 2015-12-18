hive -e "use dm_recsys_model;

drop table bought_also_bought;

CREATE TABLE bought_also_bought(
product_id         STRING comment '产品id',
also_bought_product_id      STRING comment '又买产品id',
support DOUBLE  comment '支持度',
confidence DOUBLE  comment '置信度',
correlation DOUBLE  comment '相关度'
)
COMMENT '保存产品的买了又买的数据'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;"