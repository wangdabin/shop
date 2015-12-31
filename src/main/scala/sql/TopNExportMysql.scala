package sql

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import untils.{MySqlUtils, UUID5}

/**
 * Created by wangdabin1216 on 15/11/25.
 * 转化对应hive中的top表,导入到mysql数据库
 * Source table:hive dm_recsys_model.top
 * Target table:mysql recommend.goods
 */
object TopNExportMysql {
  def main(args: Array[String]) {
    val currentTime = System.currentTimeMillis()
    val sparkConf = new SparkConf().setAppName("TopNExportMysql")
    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)

    val dbName = "recsysdmd";
    val transform_sql = "SELECT platform.type platform_type,        product.type product_type,        goods.code goods_code,        score FROM dm_recsys_model.top_n LEFT OUTER JOIN goods ON top_n.product_id = goods.productid LEFT OUTER JOIN product ON top_n.product_id = product.id LEFT OUTER JOIN platform ON goods.platformid = platform.id WHERE marketable = 1   AND isgift = 0   AND istest = 0   AND saletype = 0   AND price <> 0   "
    //1.执行对应的SQL
    println("Result of " + transform_sql + ":")
    import hiveContext.sql
    sql("use " + dbName)
    val sql_result = sql(transform_sql)

    //2.从llshopods.stock_info中加载对应的销售存量为0的,进行过滤
    // 一共涉及到6个过滤 sql中包含5个过滤(价格不为0,下架,测试,赠送,销售类型为0的),程序中过滤售罄的
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
    val mysql_result = sql_result.map(x =>{
      val platform_type = x.get(0).toString.toInt
      val product_type = x.get(1).toString.toInt
      val goods_code = x.get(2).toString.toInt
      val score = x.get(3).toString.toDouble
      val id = UUID5.fromString(platform_type + "@" + goods_code + "@" + product_type)
      val create_time = currentTime
      (id,platform_type,product_type,goods_code,score,create_time)
    }).filter(x =>{
        val platform_type = x._2
        val business_unit = transformPlatType2BusinessUnit(platform_type)
        val goods_code = x._4
        if(bfilterList.value.contains(goods_code + "$" +business_unit)){
          sell_out_filter += 1
        }
        !bfilterList.value.contains(goods_code + "$" +business_unit)
      })
    mysql_result.foreachPartition(MySqlUtils.saveRddForTransformTopN)
    //删除未更新的数据
    MySqlUtils.deleteDataForTransformTopN(currentTime);
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
