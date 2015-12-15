package com.mlearn;

/**
*★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★
*★本类由工具自动生成，请勿修改、手写      order by lkn(sjzsy-lkn@126.com) ★  
*★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★
DROP TABLE IF EXISTS product_vectors
CREATE TABLE `product_vectors` (
  `productCode` int(11) DEFAULT NULL,
  `priceRate` double DEFAULT NULL,
  `proformanceRate` double DEFAULT NULL,
  `designRate` double DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8
*/

/***/
public class ProductVectors {
	private String productcode;	//		(可空)
	private Double pricerate;	//		(可空)
	private Double performancerate;	//		(可空)
	private Double designrate;	//		(可空)

	public void setProductcode(String productcode) {
		this.productcode = productcode;
	}

	public String getProductcode() {
		return productcode;
	}

	public void setPricerate(Double pricerate) {
		this.pricerate = pricerate;
	}

	public Double getPricerate() {
		return pricerate;
	}

	public void setPerformancerate(Double performancerate) {
		this.performancerate = performancerate;
	}

	public Double getPerformancerate() {
		return performancerate;
	}

	public void setDesignrate(Double designrate) {
		this.designrate = designrate;
	}

	public Double getDesignrate() {
		return designrate;
	}

}
