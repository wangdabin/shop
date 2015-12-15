hive -e "use dm_recsys_model;

drop table top_n;

CREATE TABLE top_n(
product_id      STRING comment '产品标识',
score           DOUBLE comment '热度评分'
)
COMMENT '热门推荐'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\005'
STORED AS TEXTFILE;"