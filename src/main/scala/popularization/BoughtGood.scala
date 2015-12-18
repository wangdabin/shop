package popularization

/**
 * Created by wangdabin1216 on 15/12/8.
 *
 *
 * //订单中的商品同时购买的次数 在订单中出现的次数
(A@B,3)
(A@C,2)
(B@C,2)
(A@E,3)
(B@E,2)
(A@F,1)
(B@F,1)
(C@E,1)
(E@F,1)
(F,1)

//订单总数
5
xw
//商品在订单中出现的次数

(A,4)
(B,3)
(C,2)
(E,3)
(F,2)


//在订单中同时出现的各种商品的组合所占的比重
分母 订单数 分子  出现次数   支持度  (用来过滤阈值,来设定,放在对应的数据库字段中)
这个值用来排序
(A@B,3)  / 订单总数(5)

// Ga,Gb / Ga  Ga,Gb / Gb 所占比例
A,B   A   B          置信度 （用来排序）   分母  A or B商品在订单中出现的次数 分子  A and B 订单中的商品同时购买的次数统计
 3   3/4  3/3

 相关度 信息 （filter : >1）  也是过滤
分子  (A@B,3)  * 5 订单数
分母   (A,4) (B,3)  4*3

p(A|B)/N

p(A)/N * p(B)/N

 */

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object BoughtGood {

  def main(args: Array[String]) {
    //    if (args.length < 1) {
    //      System.err.println("Usage: <recommend nums> 推荐个数 eg: 20")
    //      System.exit(1)
    //    }
    val sparkConf = new SparkConf().setAppName("BoughtGood").set("spark.default.parallelism", "24")
    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)
    val dbName = "recsysdmd"
    import hiveContext.sql
    val bought_also_bought_sql = "select mid_2.product_code, mid3.bought_also_bought_code, mid3.platform_type, mid3.support, mid3.confidence, mid3.correlation from ( select product_id,        mid_1.bought_also_bought_code bought_also_bought_code,        mid_1.platform_type platform_type,        support,        confidence,        correlation from dm_recsys_model.bought_also_bought left outer join (SELECT mid_purchase.productid productid,        mid_purchase.code bought_also_bought_code,        platform.type platform_type FROM   (SELECT distinct productid,code,platformid    FROM purchase    LEFT OUTER JOIN goods ON purchase.goodsid = goods.id) mid_purchase LEFT OUTER JOIN platform ON mid_purchase.platformid = platform.id) mid_1 on bought_also_bought.also_bought_product_id = mid_1.productid ) mid3 join (SELECT mid_purchase.productid productid,        mid_purchase.code product_code,        platform.type platform_type FROM   (SELECT distinct productid,code,platformid    FROM purchase    LEFT OUTER JOIN goods ON purchase.goodsid = goods.id    ) mid_purchase LEFT OUTER JOIN platform ON mid_purchase.platformid = platform.id) mid_2 on (mid3.product_id = mid_2.productid and mid3.platform_type = mid_2.platform_type)"
    println("Result of " + bought_also_bought_sql + ":")
    sql("use " + dbName)
    val boughtAlsoBoughtResult = sql(bought_also_bought_sql)

    val mysql_result = boughtAlsoBoughtResult.map(x =>{
      val product_code = x.get(0).toString
      val bought_also_bought_code = x.get(1).toString
      val platform_type =  x.get(2).toString.toInt
      val support =  x.get(3).toString.toDouble
      val confidence = x.get(4).toString.toDouble
      val correlation = x.get(5).toString.toDouble
      (product_code,bought_also_bought_code,platform_type,support,confidence,correlation)
    })
    overwriteTextFile("popular/product",mysql_result)
  }
  def deletePath(sc: SparkContext, path: String): Unit = {
    val hdfs = org.apache.hadoop.fs.FileSystem.get(sc.hadoopConfiguration)
    val hdfsPath = new org.apache.hadoop.fs.Path(path)
    if (hdfs.exists(hdfsPath))
      hdfs.delete(hdfsPath, true)
  }

  def overwriteTextFile[T](path: String, rdd: RDD[T]): Unit = {
    deletePath(rdd.context, path)
    rdd.saveAsTextFile(path)
  }

//  def doubleFormat(x:Double):Double = {
//    (x*10000).toInt/10000d
//  }

}
