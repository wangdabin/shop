hive -e "use recsysdmd;
CREATE TABLE model_cf(
user_id         STRING comment '用户标识',
product_id      STRING comment '产品标识',
score           DOUBLE comment '推荐评分'
)
COMMENT '基于矩阵分解的协同过滤推荐之面向用户推荐'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;"