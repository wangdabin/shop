package sql

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import untils.{MySqlUtils, UUID5}

/**
 * 转化对应hive中的goods表,导入到mysql数据库
 * Source table:hive recsysdmd.goods
 * Target table:mysql recsys02.goods
 */
object GoodsExportMysqlNew {
  def main(args: Array[String]) {
    val currentTime = System.currentTimeMillis()
    val sparkConf = new SparkConf().setAppName("GoodsExportMysqlNew")
    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)

    val dbName = "recsysdmd";
//    val transform_sql = "select  platform.type platform_type,goods.code goods_code,  product.type product_type from goods left outer join product on goods.productid = product.id left outer join platform on goods.platformid = platform.id where marketable = 1 AND isgift = 0  AND istest = 0"
    val transform_sql = "SELECT platform.type platform_type,        goods.code goods_code,        product.type product_type,        product.business business FROM goods LEFT OUTER JOIN product ON goods.productid = product.id LEFT OUTER JOIN platform ON goods.platformid = platform.id WHERE marketable = 1   AND isgift = 0   AND istest = 0   AND goods.code IS NOT NULL   AND product.type IS NOT NULL"
    //1.执行对应的SQL
    println("Result of " + transform_sql + ":")
    import hiveContext.sql
    sql("use " + dbName)
    val sql_result = sql(transform_sql)
    val mysql_result = sql_result.map(x =>{
      val platform_type = x.get(0).toString.toInt
      val goods_code = x.get(1).toString.toInt
      val product_type = x.get(2).toString.toInt
      val business  = x.get(3).toString
      val id = UUID5.fromString(platform_type + "-" + goods_code + "-" + product_type)
      val create_time = currentTime
      (id,platform_type,goods_code,product_type,business,create_time)
    })
    mysql_result.foreachPartition(MySqlUtils.saveRddForGoodsNew)
    //删除未更新的数据
    MySqlUtils.deleteDataForGoodsNew(currentTime);
  }

}
