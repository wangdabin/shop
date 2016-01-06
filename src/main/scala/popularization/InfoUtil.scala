package popularization

import org.paukov.combinatorics.Factory
import scala.collection.JavaConverters._

/**
 * Created by wangdabin1216 on 16/1/5.
 * 计算对应的支持度,可行度,并进行排序
 */
object InfoUtil {
  def  computeSCC(original_data:org.apache.spark.rdd.RDD[(String,String)],limit:Double): Array[(String,String,Double,Double,Double)] = {
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
    //1、订单总数
    val totalOrders = productOnOrder.count()
    //2、统计各类 A->B 出现的次数
    val count_double = data_mid.filter(_.length == 2).map(x => {
      (x.mkString("@"), 1)
    }).reduceByKey(_ + _).cache() //将中间结果缓存到内存
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
//      val correlation = (product2Num.get(_1).get + product2Num.get(_2).get - x._2.toDouble ) * totalOrders.toDouble / product2Num.get(_1).get / product2Num.get(_2).get

            val correlation = x._2 * totalOrders.toDouble / product2Num.get(_1).get / product2Num.get(_2).get

      //相关度计算2
//            val correlation = x._2.toDouble /Math.sqrt((product2Num.get(_1).get + product2Num.get(_2).get - x._2.toDouble ))

      //      List((_1,(_2,support,confidenceP_B_A,correlation)),(_2,(_1,support,confidenceP_A_B,correlation)))
      //      List((_2,(_1,support,confidenceP_B_A,correlation)),(_1,(_2,support,confidenceP_A_B,correlation)))
      List((_1, _2, support, confidenceP_B_A, correlation), (_2, _1, support, confidenceP_A_B, correlation))
    })
    //进行过滤


    //总的
    val total_num = productResult.count()
    println(total_num)
    val filter_result = productResult.filter(x => {
      x._5 > 1 //过滤相关度 < 1的数据
    })
    //过滤之后的
    val filter_num  = filter_result.count()
    println(filter_num)
    val total_records = filter_result.count
    val limit_records = (total_records * limit).toInt
    val result = filter_result.sortBy(x => x._3, false).top(limit_records) //支持度80%的数据
    result
  }
}
