//package com.mlearn;
//
//import java.io.File;
//import java.io.FileWriter;
//import java.io.UnsupportedEncodingException;
//import java.net.URLDecoder;
//import java.sql.Connection;
//import java.sql.PreparedStatement;
//import java.sql.ResultSet;
//import java.sql.SQLException;
//import java.sql.Timestamp;
//import java.util.ArrayList;
//import java.util.Date;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.Set;
//import java.util.TreeSet;
//import java.util.UUID;
//
//import javax.swing.plaf.basic.BasicInternalFrameTitlePane.MaximizeAction;
//
//import tools.FormulaCalc;
//import tools.LoadConfigFile;
//import tools.PubTools;
//import bean.ProductHot;
//import bean.ProductVectors;
//import bean.ProductDistance;
//import bean.VisitorPreferenceModel;
//import db.C3p0Pool;
//import bean.ItemCb;
//
//public class CalcVisitorRecommendPlatform extends Thread{
//	private Map<String, ProductVectors> productMap = null;
//	private Map<String, List<String>> recommMap = null;
//	private Map<String, ArrayList<ItemCb>> recommDetailMap = null;
//	private Map<String, List<String>> productPlatformMap = null;
//	private Map<String, VisitorPreferenceModel> visitorPrefMap = null;
//	private int MaximumRecommand = 10;
//	private int ProductNum = 0;
//	private String platformid = "";
//	/**
//	 * 启动线程
//	 */
//	public void run(){
//		calcHotPro();
//	}
//
//	/**
//	 *
//	 */
//	private void calcHotPro() {
//		long starTime=System.currentTimeMillis();
//		connToMySQLProduct();
//		for(int i = 0; i< 90; i++){
//			System.out.println("Ten-Day " + i);
//			connToHiveVisitor(new Long(i));
//			System.out.println("连接Hive结束，开始向hive库中插入数据");
//			if(i%10==0){
//				saveRecommandToHive(false);
//			}
//			saveRecommandToHive(true);
//			System.out.println("向Hive插入数据结束");
//		}
//		long endTime=System.currentTimeMillis();
//		long Time=(endTime-starTime)/1000;
//		System.out.println("Time costs: "+Time);
//	}
//
//	private void connToMySQLProduct(){
//		String sql = "select product_vectors.product_id as product_id, product_vectors.price_rate as price_rate, product_vectors.performance_rate as performance_rate, product_vectors.design_rate as design_rate, info_goods.goods_platformid as platformid from product_vectors join info_goods on product_vectors.product_id=info_goods.product_id "; //NEED UPDATE
//		Connection conn = C3p0Pool.getMysqlConn();
//		PreparedStatement ps = null;
//		ResultSet rs = null;
//		productMap = new HashMap<String, ProductVectors>();
//		productPlatformMap = new HashMap<String, List<String>>();
//		try {
//			ps = conn.prepareStatement(sql);
////			ps.setString(1, platform);
//			rs = ps.executeQuery();
//			int i = 0;
//			while(rs.next()){
//				i++;
//				String productCode = rs.getString("product_id");
//				double priceRate = rs.getDouble("price_rate");
//				double performanceRate = rs.getDouble("performance_rate");
//				double designRate = rs.getDouble("design_rate");
//				String platformid = rs.getString("platformid");
//				List<String> platformList = productPlatformMap.get(platformid);
//				if(platformList == null){
//					platformList = new ArrayList<String>();
//					platformList.add(productCode);
//					productPlatformMap.put(platformid, platformList);
//				}else{
//					platformList.add(productCode);
//				}
//				ProductVectors pv = productMap.get(productCode);
//				if(pv == null){
//					pv = new ProductVectors();
//					pv.setProductcode(productCode);
//					pv.setPricerate(priceRate);
//					pv.setPerformancerate(performanceRate);
//					pv.setDesignrate(designRate);
//					productMap.put(productCode, pv);
//				}
//			}
//			ProductNum = i;
//		}catch (SQLException e) {
//			e.printStackTrace();
//		} finally {
//			PubTools.closeDbConn(conn, ps, rs);
//		}
//	}
//
//	private void connToHiveVisitor(Long day){
//		String sql = "select idvisitor, price_pref, performance_pref, design_pref from recsysdmd.visitor_preference_model where dt = ? and record_time = ?";// and record_time > ?NEED UPDATE
//		Connection conn = C3p0Pool.getHiveConn();//C3p0Pool.getMysqlConn();
//		PreparedStatement ps = null;
//		ResultSet rs = null;
//		recommDetailMap = new HashMap<String, ArrayList<ItemCb>>();
//		visitorPrefMap = new HashMap<String, VisitorPreferenceModel>();
//		try {
//			ps = conn.prepareStatement(sql);
//			java.sql.Date d = new java.sql.Date(new Date().getTime());
//			java.sql.Date eDate = new java.sql.Date(d.getTime()-day*1L*24L*60L*60L*1000L);
////			java.sql.Date sDate = new java.sql.Date(d.getTime()-day*3L*24L*60L*60L*1000L-3L*24L*60L*60L*1000L);
//			System.out.println(d.toString().replaceAll("-", ""));
//			ps.setString(1, d.toString().replaceAll("-", ""));
//			ps.setString(2, eDate.toString().replace("-", ""));
////			ps.setString(3, sDate.toString().replace("-", ""));
//			rs = ps.executeQuery();
//			int i = 0;
//			while(rs.next()){
//				i++;
//				VisitorPreferenceModel vpm = new VisitorPreferenceModel();
//				String id = rs.getString("idvisitor");
//				vpm.setIdvisitor(id);
//				vpm.setPricePref(rs.getDouble("price_pref"));
//				vpm.setPerformancePref(rs.getDouble("performance_pref"));
//				vpm.setDesignPref(rs.getDouble("design_pref"));
//				visitorPrefMap.put(id, vpm);
//
//			}
//		}catch (SQLException e) {
//			e.printStackTrace();
//		} finally {
//			PubTools.closeDbConn(conn, ps, rs);
//		}
//		int i = 0;
//		for(String pid : productPlatformMap.keySet()){
//			System.out.println(pid);
//			platformid = pid;
//			for(String visitorID: visitorPrefMap.keySet()){
//				VisitorPreferenceModel vpm = visitorPrefMap.get(visitorID);
//				Set<ProductDistance> set = selectProduct(vpm);
//				ArrayList<ItemCb> recommandList = recommDetailMap.get(vpm.getIdvisitor());
//				i++;
//				int j = 0;
//				if(recommandList == null){
//					recommandList = new ArrayList<ItemCb>();
//					recommDetailMap.put(vpm.getIdvisitor(),recommandList);
//				}
//
//				for (ProductDistance key : set) {
//					j++;
//					if (j > MaximumRecommand || j > ProductNum){break;}
//					String productCode = key.getProductcode();
//					ItemCb icb = new ItemCb();
//					String uid = visitorID + "@" +productCode + "@" + platformid;
//					String uuid = UUID.nameUUIDFromBytes(uid.getBytes()).toString();
//					icb.setId(uuid);
//					icb.setUserId(visitorID);
//					icb.setProductId(productCode);
//					icb.setScore(key.getDistance());
//					icb.setPlatformId(platformid);
//					recommandList.add(icb);
//				}
//				recommDetailMap.put(vpm.getIdvisitor(),recommandList);
//				if (i%10000 == 0){
//					System.out.println("Platform-Visitor Processed " + i);
//				}
//				recommDetailMap.put(vpm.getIdvisitor(), recommandList);
//			}
//		}
//
//
//
//	}
//	private Double calcDistance(VisitorPreferenceModel vpm, ProductVectors pv){
//		Double result = 0.0;
//		result += (vpm.getPricePref()-pv.getPricerate())*(vpm.getPricePref()-pv.getPricerate());
//		result += (vpm.getPerformancePref()-pv.getPerformancerate())*(vpm.getPerformancePref()-pv.getPerformancerate());
//		result += (vpm.getDesignPref()-pv.getDesignrate())*(vpm.getDesignPref()-pv.getDesignrate());
//		result = Math.sqrt(result);
////		System.out.println(result);
//		return result;
//
//	}
//
//	private Set<ProductDistance> selectProduct(VisitorPreferenceModel vpm){
//		Set<ProductDistance> set = new TreeSet<ProductDistance>();
//		for(String id : productPlatformMap.get(platformid)){
////			System.out.println(id);
//			Double distance = calcDistance(vpm, productMap.get(id));
//			ProductDistance pDistance = new ProductDistance();
//			pDistance.setProductcode(id);
//			pDistance.setDistance(distance);
//			set.add(pDistance);
//		}
//		return set;
//	}
//
//	private void saveDataToDatabase() {
//		String insertSql = "INSERT INTO item_cb(id,user_id,product_id,product_platform,score,create_time) VALUES(?,?,?,?,?) ON DUPLICATE KEY UPDATE score = ? ,create_time= ?";
//		Connection conn = C3p0Pool.getMysqlConn();
//		try {
//			PreparedStatement ps = conn.prepareStatement(insertSql);
//			conn.setAutoCommit(false);
//			int i = 0;
//			int j = 0;
//			for (String id : recommDetailMap.keySet()) {
//				i++;
//				for(ItemCb itc : recommDetailMap.get(id)){
//					j++;
//					visitorRecomEntrySave(ps, itc);
//					if(j% 1000 == 0){
//						ps.executeBatch();
//						conn.commit();
//					}
//				}
//				if(i % 1000 == 0){
//					System.out.println("执行了 " + i);
//				}
//			}
//			ps.executeBatch();
//			conn.commit();
//		} catch (SQLException e) {
//			PubTools.connRollBack(conn);
//			e.printStackTrace();
//		} finally{
//			PubTools.closeConn(conn);
//		}
//	}
//	private void saveRecommandToHive(boolean append){
//		java.sql.Date date = new java.sql.Date(new Date().getTime());
//	    String dt = date.toString().replaceAll("-", "");
//		int i = 0;
//		int j = 0;
//		long time = 0;
//		time = date.getTime()/1000L;
//		String dir = System.getProperty("user.dir");
//		String outputFile = dir+"/doc/visitorRecommand"+dt+".txt";
//		try{
//
//			if(append == false){
//				loadRecommandToHive(outputFile, dt);
//			}
//			FileWriter fr=new FileWriter(outputFile,append);
//			Set<String> keySet = recommDetailMap.keySet();
//			for (String key : keySet) {
//				ArrayList<ItemCb> itemList = recommDetailMap.get(key);
//				j++;
//				for(ItemCb item : itemList){
//					fr.write(item.getId()+'\t'+key+'\t'+item.getPlatformId()+'\t'+item.getProductId()+'\t'+item.getScore()+'\t'+time);
//					fr.write("\n");
//					i++;
//					if(i%1000==0){
//						fr.flush();
//					}
//				}
//				if(j%1000 == 0){
//					System.out.println(j + "Visitors Loaded");
//				}
//			}
//			fr.flush();
//			fr.close();
//
//		}catch(Exception e){
//			e.printStackTrace();
//
//		}
//	}
//	private void loadRecommandToHive(String OutputFile, String dt) throws SQLException{
//		Connection conn = C3p0Pool.getHiveConn();
//		String loadSql = "LOAD DATA LOCAL INPATH ? INTO TABLE recsysdmd.item_cb PARTITION (dt = ?)";
//		PreparedStatement ps = conn.prepareStatement(loadSql);
//		ps.setString(1, OutputFile);
//		ps.setString(2, dt);
//		ps.executeUpdate();
//		ps.close();
//		PubTools.closeConn(conn);
//
//	}
//
//	/**
//	 * 添加操作
//	 * @param conn
//	 * @param hot
//	 * @throws SQLException
//	 */
//	private void visitorRecomEntrySave(PreparedStatement ps, ItemCb itc) throws SQLException {
//
//		ps.setString(1, itc.getId());
//		ps.setString(2, itc.getUserId());
//		ps.setString(3, itc.getProductId());
//		ps.setString(4, platformid);
//		ps.setDouble(5, itc.getScore());
//		ps.setLong(6, new Date().getTime()/1000L);
//		ps.setDouble(7, itc.getScore());
//		ps.setLong(8, new Date().getTime()/1000L);
////		ps.executeUpdate();
//		ps.addBatch();
//	}
//
//}
