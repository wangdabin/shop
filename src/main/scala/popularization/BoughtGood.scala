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

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import untils.UUID5

object BoughtGood {

  def main(args: Array[String]) {
    val currentTime = System.currentTimeMillis()
    val sparkConf = new SparkConf().setAppName("BoughtGood").set("spark.default.parallelism", "24")
    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)
    val dbName = "recsysdmd"
    import hiveContext.sql
    val bought_also_bought_sql = "SELECT mid_2.product_code,        mid3.bought_also_bought_code,        mid3.platform_type,        mid3.support,        mid3.confidence,        mid3.correlation FROM   (SELECT product_id,           mid_1.bought_also_bought_code bought_also_bought_code,           mid_1.platform_type platform_type,           support,           confidence,           correlation    FROM dm_recsys_model.bought_also_bought    JOIN      (SELECT DISTINCT productid,                       code bought_also_bought_code,                       platform.type platform_type       FROM         ( SELECT *          FROM goods          WHERE marketable = 1            AND isgift = 0            AND istest = 0            AND saletype = 0            AND price <> 0 ) goods       JOIN platform ON goods.platformid = platform.id) mid_1 ON bought_also_bought.also_bought_product_id = mid_1.productid) mid3 JOIN   (SELECT DISTINCT productid,                    code product_code,                    platform.type platform_type    FROM      ( SELECT *       FROM goods       WHERE marketable = 1         AND isgift = 0         AND istest = 0         AND saletype = 0         AND price <> 0 ) goods    JOIN platform ON goods.platformid = platform.id) mid_2 ON (mid3.product_id = mid_2.productid                                                               AND mid3.platform_type = mid_2.platform_type)"
    println("Result of " + bought_also_bought_sql + ":")
    sql("use " + dbName)
    val filter_sql = "select product_code,    business_unit from llshopods.stock_info where  sales_number = 0"
    val sql_filter_result = sql(filter_sql)
    //将存量为0的数据转化为内存List
    val filterList = sql_filter_result.map(x => {
      val product_code = x.get(0).toString
      val business_unit = x.get(1).toString // --Guanwang、Roaming、EPP、Think、不区分平台
      //      (product_code, business_unit)// -- 商品编号      --和goodsinfoes的code
      product_code + "$" +business_unit// -- 商品编号      --和goodsinfoes的code
    }).collect.distinct

    val bfilterList = sc.broadcast(filterList)
    val sell_out_filter = sc.accumulator(0, "Sell out")


    val boughtAlsoBoughtResult = sql(bought_also_bought_sql)
    val mid_data = boughtAlsoBoughtResult.map(x =>{
      val product_code = x.get(0).toString
      val bought_also_bought_code = x.get(1).toString
      val platform_type =  x.get(2).toString.toInt
      val support =  x.get(3).toString.toDouble
      val confidence = x.get(4).toString.toDouble
      val correlation = x.get(5).toString.toDouble
      ((product_code , platform_type),(bought_also_bought_code,support,correlation,confidence))
    }).filter(x =>{
      val platform_type = x._1._2
      val business_unit = transformPlatType2BusinessUnit(platform_type)
      val goods_code = x._1._1
      val bought_also_bought_code = x._2._1
      if(bfilterList.value.contains(goods_code + "$" +business_unit)|| bfilterList.value.contains(bought_also_bought_code + "$" +business_unit)){
        sell_out_filter += 1
      }
      !(bfilterList.value.contains(goods_code + "$" +business_unit)|| bfilterList.value.contains(bought_also_bought_code + "$" +business_unit))
    }).filter( x => x._2._3 >= 1).groupByKey().map( x => {
      val rec_list = x._2.toList.sortWith((x,y) => x._4 > y._4)
      (x._1,confidenceSortCut(rec_list))
    })


    val mysql_result = mid_data.map(x =>{
      val create_time = currentTime
      val platform_type = x._1._2
      val goods_code = x._1._1
      val goods_codes = x._2
      val action_type = 0
      val id = UUID5.fromString(goods_code+"@"+platform_type+"@"+action_type)
      (id,platform_type,goods_code,goods_codes,action_type,create_time)
    })

    mysql_result.saveAsTextFile("test2")
    sc.stop()
  }

  def confidenceSortCut(input:List[(String,Double,Double,Double)]) : String = {
    val requiredLen = (input.size*0.8).toInt
    val dropList = input.dropRight(requiredLen).sortWith((x,y) => x._2>y._2)
    val result = dropList.map( x => x._1+":"+x._2).mkString(",")
    result
  }
  //完成对应的平台类型和业务单元的转化
  def transformPlatType2BusinessUnit(plat_type: Int): String = {
    if (plat_type >= 11 && plat_type <= 14) {
      return "Guanwang"
    } else if (plat_type >= 21 && plat_type <= 24) {
      return "Think"
    } else if (plat_type >= 31 && plat_type <= 34) {
      return "EPP"
    } else {
      throw new RuntimeException("转化规则有误,请检查!")
    }
  }
}
