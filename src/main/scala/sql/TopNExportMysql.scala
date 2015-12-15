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
    val transform_sql = "select  platform.type platform_type,    product.type product_type,         goods.code goods_code,         score from dm_recsys_model.top_n left outer join goods on top_n.product_id = goods.productid left outer join product on top_n.product_id = product.id left outer join platform on goods.platformid = platform.id where marketable = 1 AND isgift = 0  AND istest = 0 AND saletype = 0"
    //1.执行对应的SQL
    println("Result of " + transform_sql + ":")
    import hiveContext.sql
    sql("use " + dbName)
    val sql_result = sql(transform_sql)
    val mysql_result = sql_result.map(x =>{
      val platform_type = x.get(0).toString.toInt
      val product_type = x.get(1).toString.toInt
      val goods_code = x.get(2).toString.toInt
      val score = x.get(3).toString.toDouble
      val id = UUID5.fromString(platform_type + "@" + goods_code + "@" + product_type)
      val create_time = currentTime
      (id,platform_type,product_type,goods_code,score,create_time)
    })
    mysql_result.foreachPartition(MySqlUtils.saveRddForTransformTopN)
    //删除未更新的数据
    MySqlUtils.deleteDataForTransformTopN(currentTime);
  }
}
