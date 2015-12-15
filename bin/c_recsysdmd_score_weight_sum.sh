hive -e "use dm_recsys_model;
CREATE TABLE score_weight_sum(
user_id         STRING comment '用户id',
product_id      STRING comment '产品id',
score           DOUBLE comment '评分'
)
COMMENT '保存加权之后的用户综合评分数据'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\005'
STORED AS TEXTFILE;"