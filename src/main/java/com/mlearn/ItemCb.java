package com.mlearn;

/**
*★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★
*★本类由工具自动生成，请勿修改、手写      order by lkn(sjzsy-lkn@126.com) ★  
*★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★
DROP TABLE IF EXISTS item_cb
CREATE TABLE `item_cb` (
  `id` varchar(36) NOT NULL,
  `user_id` varchar(36) DEFAULT NULL,
  `product_id` varchar(36) DEFAULT NULL,
  `score` double DEFAULT NULL,
  `create_time` datetime DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
*/

/***/
public class ItemCb {
	private String id;	//		primary key	(非空)
	private String userId;	//		(可空)
	private String productId;	//		(可空)
	private Double score;	//		(可空)
	private java.util.Date createTime;	//		(可空)
	private String platformId;

	public void setId(String id) {
		this.id = id;
	}

	public String getId() {
		return id;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public String getUserId() {
		return userId;
	}

	public void setProductId(String productId) {
		this.productId = productId;
	}

	public String getProductId() {
		return productId;
	}

	public void setScore(Double score) {
		this.score = score;
	}

	public Double getScore() {
		return score;
	}

	public void setCreateTime(java.util.Date createTime) {
		this.createTime = createTime;
	}

	public java.util.Date getCreateTime() {
		return createTime;
	}
	
	public void setPlatformId(String platformId) {
		this.platformId = platformId;
	}

	public String getPlatformId() {
		return platformId;
	}

}
