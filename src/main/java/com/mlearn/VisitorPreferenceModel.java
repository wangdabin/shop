package com.mlearn;

import java.util.Set;
import java.util.TreeSet;

/**
*★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★
*★本类由工具自动生成，请勿修改、手写      order by lkn(sjzsy-lkn@126.com) ★  
*★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★
DROP TABLE IF EXISTS visitor_preference_model
CREATE TABLE `visitor_preference_model` (
  `idvisitor` bigint(20) DEFAULT NULL,
  `price_pref` double DEFAULT NULL,
  `performance_pref` double DEFAULT NULL,
  `design_pref` double DEFAULT NULL,
  `update_time` datetime DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8
*/

/***/
public class VisitorPreferenceModel {
	private String idvisitor;	//		(可空)
	private Double pricePref;	//		(可空)
	private Double performancePref;	//		(可空)
	private Double designPref;	//		(可空)
	private java.util.Date updateTime;	//		(可空)
    private Double weightParameter;
	public void setIdvisitor(String idvisitor) {
		this.idvisitor = idvisitor;
	}

	public String getIdvisitor() {
		return idvisitor;
	}

	public void setPricePref(Double pricePref) {
		this.pricePref = pricePref;
	}

	public Double getPricePref() {
		return pricePref;
	}

	public void setPerformancePref(Double performancePref) {
		this.performancePref = performancePref;
	}

	public Double getPerformancePref() {
		return performancePref;
	}

	public void setDesignPref(Double designPref) {
		this.designPref = designPref;
	}

	public Double getDesignPref() {
		return designPref;
	}

	public void setUpdateTime(java.util.Date updateTime) {
		this.updateTime = updateTime;
	}

	public java.util.Date getUpdateTime() {
		return updateTime;
	}
	
	public void setWeightParameter(Double weightParameter) {
		this.weightParameter = weightParameter;
	}

	public Double getWeightParameter() {
		return weightParameter;
	}


}
