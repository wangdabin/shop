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
import org.paukov.combinatorics.Factory

import scala.collection.JavaConverters._

object BoughtProduct {

  def main(args: Array[String]) {
    //    if (args.length < 1) {
    //      System.err.println("Usage: <recommend nums> 推荐个数 eg: 20")
    //      System.exit(1)
    //    }
    val sparkConf = new SparkConf().setAppName("PopularizationRecom").set("spark.default.parallelism", "24")
    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)
    val dbName = "recsysdmd"
    import hiveContext.sql
    val purchase_sql = "select orderid,productid from purchase left outer join goods on purchase.goodsid = goods.id"

    println("Result of " + purchase_sql + ":")
    sql("use " + dbName)
    val sql_result = sql(purchase_sql)

//    val product_goods_platform = " select mid_purchase.productid,goods.code,platform.type from (select distinct(productid) from purchase left outer join goods on purchase.goodsid = goods.id) mid_purchase left outer join goods on mid_purchase.productid = goods.productid left outer join platform on goods.platformid = platform.id where platform.type is not null "
//
//    val sql_product_goods_platform = sql(product_goods_platform)


    //    val sql_result = sc.textFile("/Users/wangdabin1216/work/lenovo/sbtSpark/data/order1.txt")

    //1.拿到对应的数据
    val original_data = sql_result.map(x => {
      val orderid = x.get(0).toString
      val goodsid = x.get(1).toString
      //      val orderid =  x.split(" ")(0)
      //      val goodsid =  x.split(" ")(1)
      (orderid, goodsid)
    })
    //2.得到对应订单中的产品集合
    val productOnOrder = original_data.reduceByKey(_ + "," + _)

    val data_mid = productOnOrder.flatMap(x => {
      //得到每个订单中产品的集合
      val proudcts = x._2.split(",").distinct
      val initialVector = Factory.createVector(
        proudcts)
      var index = 2
      if (proudcts.length < 2) {
        index = proudcts.length
      }
      val result = Factory.createSimpleCombinationGenerator(initialVector, index)
      result.generateAllObjects().asScala.toList.map {
        item =>
          item.asScala.toList.sorted
      }
    })
    //    val textFile = sc.textFile("/Users/wangdabin1216/work/lenovo/sbtSpark/data/order.txt")
    //1、订单总数
    val totalOrders = productOnOrder.count()
    //    println(totalOrders)
    //得到对应的排列组合
    //    val data_mid = textFile.flatMap(x => {
    //      val arr = x.split(" ")
    //      val initialVector = Factory.createVector(
    //        arr)
    //      var index = 2
    //      if (arr.length < 2) {
    //        index = arr.length
    //      }
    //      val result = Factory.createSimpleCombinationGenerator(initialVector, index);
    //      result.generateAllObjects().asScala.toList.map {
    //        item =>
    //          item.asScala.toList.sorted
    //      }
    //    })

    //2、统计各类 A->B 出现的次数
    val count_double = data_mid.filter(_.length == 2).map(x => {
      (x.mkString("@"), 1)
    }).reduceByKey(_ + _).cache() //将中间结果缓存到内存

    //    count_double.collect().foreach(println)


    //统计各种商品在订单中出现的次数
    val product2Num = productOnOrder.flatMap(x => {
      x._2.split(",")
    }).map(x => {
      (x, 1)
    }).reduceByKey(_ + _).collectAsMap()

    //用2的结果/订单总数的到对应的结果   支持度: P(A∪B)，即A和B这两个项集在事务集D中同时出现的概率
    val productResult = count_double.flatMap(x => {
      val key = x._1 //(A@B)
      val support = x._2.toDouble / totalOrders //支持度
      val _1 = x._1.split("@")(0)
      val _2 = x._1.split("@")(1)
      //置信度1: P(B｜A)，即在出现项集A的事务集D中，项集B也同时出现的概率。
      val confidenceP_B_A = x._2.toDouble / product2Num.get(_1).get
      //置信度2: P(A｜B)，即在出现项集A的事务集D中，项集B也同时出现的概率。
      val confidenceP_A_B = x._2.toDouble / product2Num.get(_2).get
      //相关度 p(A|B)/N   / p(A)/N * p(B)/N
      val correlation = x._2 * totalOrders.toDouble / product2Num.get(_1).get / product2Num.get(_2).get
//      List((_1,(_2,support,confidenceP_B_A,correlation)),(_2,(_1,support,confidenceP_A_B,correlation)))
//      List((_2,(_1,support,confidenceP_B_A,correlation)),(_1,(_2,support,confidenceP_A_B,correlation)))
      List(_1+"\t"+_2+"\t"+support+"\t"+confidenceP_B_A+"\t"+correlation,
        _2+"\t"+_1+"\t"+support+"\t"+confidenceP_A_B+"\t"+correlation)
    })
    overwriteTextFile("popular/product",productResult)

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
