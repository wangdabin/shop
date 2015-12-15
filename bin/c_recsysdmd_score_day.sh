hive -e "use dm_recsys_model;

drop table user_product_score_d;

CREATE TABLE user_product_score_d(
user_id         STRING comment '用户id',
product_id      STRING comment '产品id',
score           DOUBLE comment '评分'
)
COMMENT '保存每一天用户对产品的评分数据'
PARTITIONED BY (dt STRING COMMENT '日期分区')
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\005'
STORED AS TEXTFILE;"