package untils

import java.sql.{Connection, PreparedStatement}

/**
 * Created by wangdabin1216 on 15/11/10.
 */
object MySqlUtils {

  def saveRddForALS(iterator: Iterator[(String,String,String,Double,Long)]): Unit = {
    if(iterator.isEmpty){
      return
    }
    var conn: Connection = null
    var ps: PreparedStatement = null
    val saveOrUpdate_sql = "insert into model_cf(id,user_id,product_id,score,create_time) values (?,?,?,?,?) ON DUPLICATE KEY UPDATE user_id=VALUES(user_id), product_id=VALUES(product_id), score=VALUES(score), create_time=VALUES(create_time)"
    try {
      conn = ConnectionFactory.connect()
      iterator.foreach(data => {
        ps = conn.prepareStatement(saveOrUpdate_sql)
        ps.setString(1, data._1)
        ps.setString(2, data._2)
        ps.setString(3,data._3)
        ps.setDouble(4,data._4)
        ps.setLong(5,data._5)
        ps.executeUpdate()
      })
//      ps.executeBatch() //执行批处理
    }  finally {
      if (ps != null) {
        ps.close()
      }
      if (conn != null) {
        conn.close()
      }
    }
  }

  def deleteDataForALS(createTime:Long): Unit = {
    var conn: Connection = null
    var ps: PreparedStatement = null
    val delete_sql = "delete from model_cf where create_time < ?"
    try {
        conn = ConnectionFactory.connect()
        ps = conn.prepareStatement(delete_sql)
        ps.setLong(1, createTime)
        ps.execute() //执行删除
    } catch {
      case e: Exception => println("Mysql Exception", e)
    } finally {
      if (ps != null) {
        ps.close()
      }
      if (conn != null) {
        conn.close()
      }
    }
  }



  def saveRddForTop(iterator: Iterator[(String,Int,Int,Int,Int,Long)]): Unit = {
    if(iterator.isEmpty){
      return
    }
    var conn: Connection = null
    var ps: PreparedStatement = null
    val saveOrUpdate_sql = "insert into top_n(id,platform_type,product_type,view_number,sale_number,create_time) values (?,?,?,?,?,?) ON DUPLICATE KEY UPDATE platform_type=VALUES(platform_type), product_type=VALUES(product_type), view_number=VALUES(view_number),sale_number=VALUES(sale_number), create_time=VALUES(create_time)"
    try {
      conn = ConnectionFactory.connect()
      iterator.foreach(data => {
        ps = conn.prepareStatement(saveOrUpdate_sql)
        ps.setString(1, data._1)
        ps.setInt(2, data._2)
        ps.setInt(3,data._3)
        ps.setInt(4,data._4)
        ps.setInt(5,data._5)
        ps.setLong(6,data._6)
        ps.executeUpdate()
      })
      //      ps.executeBatch() //执行批处理
    } catch {
      case e: Exception => println("Mysql Exception", e)
    } finally {
      if (ps != null) {
        ps.close()
      }
      if (conn != null) {
        conn.close()
      }
    }
  }

  def deleteDataForTop(createTime:Long): Unit = {
    var conn: Connection = null
    var ps: PreparedStatement = null
    val delete_sql = "delete from top_n where create_time < ?"
    try {
      conn = ConnectionFactory.connect()
      ps = conn.prepareStatement(delete_sql)
      ps.setLong(1, createTime)
      val result  = ps.executeUpdate(delete_sql) //执行删除
      println("共删除旧数据:" + result)
    } catch {
      case e: Exception => println("Mysql Exception", e)
    } finally {
      if (ps != null) {
        ps.close()
      }
      if (conn != null) {
        conn.close()
      }
    }
  }



  def saveRddForCfMfUser(iterator: Iterator[(String,String,String,Int,String,Long)]): Unit = {
    if(iterator.isEmpty){
      return
    }
    var conn: Connection = null
    var ps: PreparedStatement = null
    val insert_sql = "insert into cf_mf_user(id,lenovo_id,le_id,platform_type,goods_codes,create_time) values (?,?,?,?,?,?) ON DUPLICATE KEY UPDATE lenovo_id=VALUES(lenovo_id), le_id=VALUES(le_id), platform_type=VALUES(platform_type), goods_codes=VALUES(goods_codes),create_time=VALUES(create_time)"
    try {
      conn = ConnectionFactory.connect()
      ps = conn.prepareStatement(insert_sql)

      for (eachRow <- iterator) {
        ps.setString(1, eachRow._1)
        ps.setString(2, eachRow._2)
        ps.setString(3, eachRow._3)
        ps.setInt(4, eachRow._4)
        ps.setString(5,eachRow._5)
        ps.setLong(6,eachRow._6)
        ps.addBatch()
      }
      ps.executeBatch()
    }  finally {
      if (ps != null) {
        ps.close()
      }
      if (conn != null) {
        conn.close()
      }
    }
  }

  def deleteDataForCfMfUser(createTime:Long): Unit = {
    var conn: Connection = null
    var ps: PreparedStatement = null
    val delete_sql = "delete from cf_mf_user where create_time < ?"
    try {
      conn = ConnectionFactory.connect()
      ps = conn.prepareStatement(delete_sql)
      ps.setLong(1, createTime)
      val result  = ps.executeUpdate() //执行删除
      println("共删除旧数据:" + result)

    } finally {
      if (ps != null) {
        ps.close()
      }
      if (conn != null) {
        conn.close()
      }
    }
  }



  def saveRddForGoods(iterator: Iterator[(String,Int,Int,Int,Long)]): Unit = {
    if(iterator.isEmpty){
      return
    }
    var conn: Connection = null
    var ps: PreparedStatement = null
    val saveOrUpdate_sql = "insert into goods(id,platform_type,goods_code,product_type,create_time) values (?,?,?,?,?) ON DUPLICATE KEY UPDATE platform_type=VALUES(platform_type), goods_code=VALUES(goods_code), product_type=VALUES(product_type), create_time=VALUES(create_time)"
    try {
      conn = ConnectionFactory.connect()
      iterator.foreach(data => {
        ps = conn.prepareStatement(saveOrUpdate_sql)
        ps.setString(1, data._1)
        ps.setInt(2, data._2)
        ps.setInt(3,data._3)
        ps.setInt(4,data._4)
        ps.setLong(5,data._5)
        ps.executeUpdate()
      })
      //      ps.executeBatch() //执行批处理
    } finally {
      if (ps != null) {
        ps.close()
      }
      if (conn != null) {
        conn.close()
      }
    }
  }


  def saveRddForGoodsNew(iterator: Iterator[(String,Int,Int,Int,String,Long)]): Unit = {
    if(iterator.isEmpty){
      return
    }
    var conn: Connection = null
    var ps: PreparedStatement = null
    val saveOrUpdate_sql = "insert into goods(id,platform_type,goods_code,product_type,business,create_time) values (?,?,?,?,?,?) ON DUPLICATE KEY UPDATE platform_type=VALUES(platform_type), goods_code=VALUES(goods_code), product_type=VALUES(product_type), business=VALUES(business),create_time=VALUES(create_time)"
    try {
      conn = ConnectionFactoryNew.connect()
      iterator.foreach(data => {
        ps = conn.prepareStatement(saveOrUpdate_sql)
        ps.setString(1, data._1)
        ps.setInt(2, data._2)
        ps.setInt(3,data._3)
        ps.setInt(4,data._4)
        ps.setString(5,data._5)
        ps.setLong(6,data._6)
        ps.executeUpdate()
      })
      //      ps.executeBatch() //执行批处理
    } finally {
      if (ps != null) {
        ps.close()
      }
      if (conn != null) {
        conn.close()
      }
    }
  }

  def deleteDataForGoods(createTime:Long): Unit = {
    var conn: Connection = null
    var ps: PreparedStatement = null
    val delete_sql = "delete from goods where create_time < ?"
    try {
      conn = ConnectionFactory.connect()
      ps = conn.prepareStatement(delete_sql)
      ps.setLong(1, createTime)
      val result  = ps.executeUpdate() //执行删除
      println("共删除旧数据:" + result)
    } finally {
      if (ps != null) {
        ps.close()
      }
      if (conn != null) {
        conn.close()
      }
    }
  }

  def deleteDataForGoodsNew(createTime:Long): Unit = {
    var conn: Connection = null
    var ps: PreparedStatement = null
    val delete_sql = "delete from goods where create_time < ?"
    try {
      conn = ConnectionFactoryNew.connect()
      ps = conn.prepareStatement(delete_sql)
      ps.setLong(1, createTime)
      val result  = ps.executeUpdate() //执行删除
      println("共删除旧数据:" + result)
    } finally {
      if (ps != null) {
        ps.close()
      }
      if (conn != null) {
        conn.close()
      }
    }
  }


  def saveRddForTransformTopN(iterator: Iterator[(String,Int,Int,Int,Double,Long)]): Unit = {
    if(iterator.isEmpty){
      return
    }
    var conn: Connection = null
    var ps: PreparedStatement = null
    val saveOrUpdate_sql = "insert into top_n(id,platform_type,product_type,goods_code,score,create_time) values (?,?,?,?,?,?) ON DUPLICATE KEY UPDATE platform_type=VALUES(platform_type), product_type=VALUES(product_type), goods_code=VALUES(goods_code), score=VALUES(score), create_time=VALUES(create_time)"
    try {
      conn = ConnectionFactory.connect()
      iterator.foreach(data => {
        ps = conn.prepareStatement(saveOrUpdate_sql)
        ps.setString(1, data._1)
        ps.setInt(2, data._2)
        ps.setInt(3,data._3)
        ps.setInt(4,data._4)
        ps.setDouble(5,data._5)
        ps.setLong(6,data._6)
        ps.executeUpdate()
      })
    } finally {
      if (ps != null) {
        ps.close()
      }
      if (conn != null) {
        conn.close()
      }
    }
  }

  def deleteDataForTransformTopN(createTime:Long): Unit = {
    var conn: Connection = null
    var ps: PreparedStatement = null
    val delete_sql = "delete from top_n where create_time < ?"
    try {
      conn = ConnectionFactory.connect()
      ps = conn.prepareStatement(delete_sql)
      ps.setLong(1, createTime)
      val result  = ps.executeUpdate() //执行删除
      println("共删除旧数据:" + result)
    } finally {
      if (ps != null) {
        ps.close()
      }
      if (conn != null) {
        conn.close()
      }
    }
  }

  def main(args: Array[String]) {
    //    val conf = new SparkConf().setAppName("RDDToMysql").setMaster("local")
    //    val sc = new SparkContext(conf)
    //    val data = sc.parallelize(List(("www", "www"), ("iteblog","wwww"), ("com", "wwwww")))
    //    data.foreachPartition(toMysql)

  }
}
