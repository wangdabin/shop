package sql

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import untils.{MySqlUtils, UUID5}

/**
 * Created by wangdabin1216 on 15/11/23.
 */
object CompositeGrade {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: <recommend nums> 推荐个数 eg: 20")
      System.exit(1)
    }
    val recommend_nums = args(0).toInt
    val sparkConf = new SparkConf().setAppName("CompositeGrade").set("spark.default.parallelism", "24")
    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)
    import hiveContext.sql
    val dbName = "recsysdmd"
    val currentTime = System.currentTimeMillis()

//    val composite_sql = "SELECT model_cf.user_id user_id,        user.lenovoid lenovo_id,        user.cookie le_id,        platform.type platform_type,        goods.code goods_code,        model_cf.score score FROM dm_recsys_model.model_cf LEFT OUTER JOIN user ON model_cf.user_id = user.id LEFT OUTER JOIN goods ON model_cf.product_id = goods.productid LEFT OUTER JOIN platform ON goods.platformid = platform.id WHERE goods.marketable = 1 AND goods.saletype = 0"
    val composite_sql = "SELECT mid.lenovo_id lenovo_id,        mid.le_id le_id,        mid.platform_type platform_type,        mid.goods_code goods_code,        mid.score score FROM   (SELECT model_cf.user_id user_id,           model_cf.product_id product_id,           user.lenovoid lenovo_id,           user.cookie le_id,           platform.type platform_type,           goods.code goods_code,           model_cf.score score    FROM dm_recsys_model.model_cf    LEFT OUTER JOIN USER ON model_cf.user_id = USER.id    LEFT OUTER JOIN goods ON model_cf.product_id = goods.productid    LEFT OUTER JOIN platform ON goods.platformid = platform.id    WHERE goods.marketable = 1      AND goods.saletype = 0      AND goods.price <> 0      AND goods.isgift = 0      AND goods.istest = 0 ) mid LEFT OUTER JOIN dm_recsys_model.brought_product ON (mid.user_id = brought_product.user_id   AND mid.product_id = brought_product.product_id) WHERE brought_product.product_id IS NULL"
    //1.执行对应的SQL
    println("Result of " + composite_sql + ":")
    sql("use " + dbName)
    val sql_result = sql(composite_sql)
    //2.从llshopods.stock_info中加载对应的销售存量为0的,进行过滤
    // 一共涉及到5个过滤 sql中包含4个过滤(已经购买过的,价格大于0,下架,销售类型为0的),程序中过滤售罄的
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
    //2.将结果转成对应的mysql中需要的格式


    val type_filter = sc.accumulator(0, "Type contains 0")
    val sell_out_filter = sc.accumulator(0, "Sell out")
    val lenovo_users = sc.accumulator(0, "lenovo user num")
    val le_users = sc.accumulator(0, "le user num")

    val mysql_result = sql_result.map(x => {

      var lenovo_id_df = x.get(0)
      var lenovo_id = ""
      if(lenovo_id_df != null){
        lenovo_id = lenovo_id_df.toString
        lenovo_users += 1
      }
      var le_id_df = x.get(1)
      var le_id = ""
      if(le_id_df != null){
        le_id = le_id_df.toString
        le_users += 1
      }
      val platform_type = x.get(2).toString.toInt
      val goods_code = x.get(3).toString.toInt
      val score = x.get(4).toString.toDouble
      (lenovo_id,le_id,platform_type,goods_code,score)
    }).filter(x => {//添加进行过滤的条件    20151125
       if(x._3.toString.contains("0")){
         type_filter += 1
       }
       !x._3.toString.contains("0") //添加过滤条件1,将对应的平台号包含0的过滤掉
    }).filter(x =>{
      val platform_type = x._3
      val business_unit = transformPlatType2BusinessUnit(platform_type)
      val goods_code = x._4
      if(bfilterList.value.contains(goods_code + "$" +business_unit)){
        sell_out_filter += 1
      }
      !bfilterList.value.contains(goods_code + "$" +business_unit)
    }).map(x => {
      val lenovo_id = x._1
      val le_id = x._2
      val platform_type = x._3
      val goods_code = x._4
      val score = x._5
      //替代groupbykey
      //      val key = (user_id, platform_type)
      //      (key, (goods_code, score, lenovo_id, le_id))
      //    }).groupByKey.
      val value = goods_code + "_" + score
      if(StringUtils.isNotBlank(lenovo_id)){
        val key = (lenovo_id,1,platform_type)
        (key,value)
      }else{
        val key = (le_id,0,platform_type)
        (key,value)
      }
      }).reduceByKey(_ + "@" + _)
      .map(x => {
      //根据用户id和平台类型字段生成一个uuid
      val lenOrleId = x._1._1
      val userType = x._1._2
      val platform_type = x._1._3
      val id = UUID5.fromString(lenOrleId + "@" + platform_type)
      var lenovo_id = ""
      var le_id = ""
      if(userType == 0){
        le_id = lenOrleId
      }else{
        lenovo_id = lenOrleId
      }
      val goods_codes = x._2.split("@").distinct.sortBy(-_.split("_")(1).toDouble).take(recommend_nums).map(_.split("_")(0)).mkString(";")
      (id, lenovo_id, le_id, platform_type, goods_codes, currentTime)
    })

//    mysql_result.saveAsTextFile("model_cf/result")
    //3.将对应的结果保存到mysql数据库中
    mysql_result.foreachPartition(MySqlUtils.saveRddForCfMfUser)
    //4.删除旧的数据
    MySqlUtils.deleteDataForCfMfUser(currentTime)
    println("类型为0的被过滤:" +type_filter.value)
    println("售罄商品被过滤:" + sell_out_filter.value)
    sc.stop()
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
