//package sql
///**
// * Created by wangdabin1216 on 15/11/2.
// */
//
//import org.apache.spark.sql.hive.HiveContext
//import org.apache.spark.{SparkConf, SparkContext}
//import untils.{UUID5, MySqlUtils, DateUtils}
//
//object AnalysisGoods {
//  def main(args: Array[String]) {
//
//    if (args.length < 2) {
//      System.err.println("Usage: <startTime> <stopTime>  eg: 1991-10-31 1992-10-31")
//      System.exit(1)
//    }
//    val create_time = System.currentTimeMillis()
//    val sparkConf = new SparkConf().setAppName("AnalysisGoods")
//    val sc = new SparkContext(sparkConf)
//    val hiveContext = new HiveContext(sc)
//    import hiveContext.sql
//    val dbName = "recsysdmd";
//
//    //将对应的时间加入到过滤中
//    val startTime = DateUtils.toDate(args(0)).getTime
//    val endTime = DateUtils.toDate(args(1)).getTime
//
//    //执行SQL语法,统计对应的topN
//    /*
//    SELECT CASE
//           WHEN mid_action.platform_type IS NOT NULL THEN mid_action.platform_type
//           ELSE mid1_action.platform_type
//       END AS platform_type,
//       CASE
//           WHEN mid_action.product_type IS NOT NULL THEN mid_action.product_type
//           ELSE mid1_action.product_type
//       END AS platform_type,
//       CASE
//           WHEN mid_action.view_number IS NOT NULL THEN mid_action.view_number
//           ELSE 0
//       END AS view_number,
//       CASE
//           WHEN mid1_action.sale_number IS NOT NULL THEN mid1_action.sale_number
//           ELSE 0
//       END AS sale_number
//FROM
//  (SELECT platform.type platform_type,
//          product.type product_type,
//          count(*) view_number
//   FROM
//     (SELECT *
//      FROM action
//      WHERE action.type = 1) action
//   LEFT OUTER JOIN platform ON action.platformid = platform.id
//   LEFT OUTER JOIN goods ON action.itemid = goods.id
//   LEFT OUTER JOIN product ON goods.productid = product.id
//   GROUP BY platform.type,
//            product.type) mid_action
//FULL OUTER JOIN
//  (SELECT platform.type platform_type,
//          product.type product_type,
//          count(*) sale_number
//   FROM
//     (SELECT *
//      FROM action
//      WHERE action.type = 0) action
//   LEFT OUTER JOIN platform ON action.platformid = platform.id
//   LEFT OUTER JOIN purchase ON action.itemid = purchase.orderid
//   LEFT OUTER JOIN goods ON purchase.goodsid = goods.id
//   LEFT OUTER JOIN product ON goods.productid = product.id
//   GROUP BY platform.type,product.type) mid1_action
//ON (mid_action.platform_type = mid1_action.platform_type AND mid_action.product_type = mid1_action.product_type)
//     */
//    val topn_sql ="SELECT CASE  WHEN mid_action.platform_type IS NOT NULL THEN mid_action.platform_type            ELSE mid1_action.platform_type        END AS platform_type,        CASE            WHEN mid_action.product_type IS NOT NULL THEN mid_action.product_type            ELSE mid1_action.product_type        END AS platform_type,        CASE            WHEN mid_action.view_number IS NOT NULL THEN mid_action.view_number            ELSE 0        END AS view_number,        CASE            WHEN mid1_action.sale_number IS NOT NULL THEN mid1_action.sale_number            ELSE 0        END AS sale_number FROM   (SELECT platform.type platform_type,           product.type product_type,           count(*) view_number    FROM      (SELECT *       FROM action       WHERE action.type = 1 AND action.createtime >= "+startTime+" AND action.createtime <= "+endTime+") action    LEFT OUTER JOIN platform ON action.platformid = platform.id    LEFT OUTER JOIN goods ON action.itemid = goods.id    LEFT OUTER JOIN product ON goods.productid = product.id    GROUP BY platform.type,             product.type) mid_action FULL OUTER JOIN   (SELECT platform.type platform_type,           product.type product_type,           count(*) sale_number    FROM      (SELECT *       FROM action       WHERE action.type = 0 AND action.createtime >= "+startTime+" AND action.createtime <="+endTime+") action    LEFT OUTER JOIN platform ON action.platformid = platform.id    LEFT OUTER JOIN purchase ON action.itemid = purchase.orderid    LEFT OUTER JOIN goods ON purchase.goodsid = goods.id    LEFT OUTER JOIN product ON goods.productid = product.id    GROUP BY platform.type,product.type) mid1_action ON (mid_action.platform_type = mid1_action.platform_type AND mid_action.product_type = mid1_action.product_type)"
//    //1.执行对应的SQL
//    println("Result of " + topn_sql + ":")
//    sql("use " + dbName)
//    val results = sql(topn_sql)
//    //2.将结果转成对应的mysql中需要的格式
//    val results_mysql = results.map(x =>{
//      val platform_type = x.get(0).toString.toInt
//      val product_type = x.get(1).toString.toInt
//      val view_number = x.get(2).toString.toInt
//      val sale_number = x.get(3).toString.toInt
//      val id = UUID5.fromString("" + platform_type + product_type)
//      (id,platform_type,product_type,view_number,sale_number,create_time)
//    })
//    //3.将对应的结果保存到mysql数据库中
//
//    results_mysql.foreachPartition(MySqlUtils.saveRddForTop)
//    MySqlUtils.deleteDataForTop(create_time)
//    sc.stop()
//  }
//
////  def insertResultToMysql(iterator: Array[Row]): Unit ={
////    val configs = ConfigUtils.getConfig("/config/db.properties")
////    val url = configs.get("MYSQL_DB_URL").get
////    val username = configs.get("MYSQL_DB_USERNAME").get
////    val password = configs.get("MYSQL_DB_PASSWORD").get
////    var conn: Connection = null
////    var ps: PreparedStatement = null
////    val insert_sql = "insert into statistical(goodid,ptid,ywbm,ctype,click_sum,buy_sum) values (?,?,?,?,?,?)"
////    val delete_sql = "delete from statistical where goodid = ?"
////    val update_sql = "update statistical set ptid = ?,ywbm = ?,ctype = ?,click_sum = ?,buy_sum = ? where goodid = ?"
////    val select_sql = "select goodid from statistical"
////    println("将数据更新到mysql中...")
////    try {
////
////      Class.forName("com.mysql.jdbc.Driver")
////      conn = DriverManager.getConnection(url, username, password)
////      var newSet = Set[String]()
////      var oldSet = Set[String]()
////      ps = conn.prepareStatement(select_sql)
////      val oldResult = ps.executeQuery()
////      while(oldResult.next()){
////        val goodid = oldResult.getString(1)
////        oldSet += goodid
////      }
////      iterator.foreach(data => {
////        newSet += data.get(0).toString
////      })
////      val addSet = newSet -- oldSet
////      println("新增:" + addSet.size)
////      val updateSet = newSet & oldSet
////      println("更新:" + updateSet.size)
////      val delSet = oldSet -- newSet
////      println("删除:" + delSet.size)
////      iterator.filter(x => {addSet.contains(x.get(0).toString)}).foreach(data => {
////        ps = conn.prepareStatement(insert_sql)
////        ps.setString(1,data.get(0).toString)
////        ps.setString(2,data.get(1).toString)
////        ps.setString(3,data.get(2).toString)
////        ps.setString(4,data.get(3).toString)
////        ps.setString(5, data.get(4).toString)
////        ps.setString(6,data.get(5).toString)
////        ps.executeUpdate()
////      })
////      iterator.filter(x => {updateSet.contains(x.get(0).toString)}).foreach(data => {
////        ps = conn.prepareStatement(update_sql)
////        ps.setString(1,data.get(1).toString)
////        ps.setString(2,data.get(2).toString)
////        ps.setString(3,data.get(3).toString)
////        ps.setString(4,data.get(4).toString)
////        ps.setString(5, data.get(5).toString)
////        ps.setString(6,data.get(0).toString)
////        ps.executeUpdate()
////      })
////      delSet.foreach(data => {
////        ps = conn.prepareStatement(delete_sql)
////        ps.setString(1,data)
////        ps.execute()
////      })
////    }catch {
////      case e:Exception => println(e)
////    }
////    finally {
////      if (ps != null) {
////        ps.close()
////      }
////      if (conn != null) {
////        conn.close()
////      }
////    }
////  }
//
//
//}
