package wdb

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by wangdabin1216 on 15/7/9.
 */
object program_1_lzy {

  def main(args: Array[String]) {



    /*
   *准备数据
   *@parameter
   * data : 原始数据
   * gsm : 基站位置字典表
   */
    val data = "datamine/input/hubei_cdr"
    val gsm = "datamine/input/maps/gsm.csv"
    val conf = new SparkConf().setAppName("spark1")
    val sc = new SparkContext(conf)
    val sdata = sc.textFile(gsm)

    /*
   *按用户UID进行分组，并且按照时间进行排序。
   *每一数据就是一个用户按时间点出现的所有点轨迹
   */
    val da = sc.textFile(data).map(x => x.split("\001")).filter(x => x(0) != "2")
    val user_group = da.map(x => (x(0), x(1) + "|" + x(2) + "|" + x(3))).reduceByKey(_ + "," + _)
    val user_sort = user_group.map(x => {
      val xlist = x._2.split(",", -1).toList
      val xsort = xlist.sortBy { case (y) => y.split("\\|")(0).replaceAll("[- :]", "") }
      (x._1, xsort)
    }).filter(_._2.size > 1)

    /*
   *通过两个点得经纬度来计算距离的函数方法
   */
    def calc_distance(start_lat: String, start_lng: String, end_lat: String, end_lng: String): Long = {
      val AVERAGE_RADIUS_OF_EARTH = 6371
      val userLat = start_lat.toDouble
      val userLng = start_lng.toDouble
      val venueLat = end_lat.toDouble
      val venueLng = end_lng.toDouble

      val latDistance = Math.toRadians(userLat - venueLat)
      val lngDistance = Math.toRadians(userLng - venueLng)

      val a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2) +
        Math.cos(Math.toRadians(userLat)) * Math.cos(Math.toRadians(venueLat)) *
          Math.sin(lngDistance / 2) * Math.sin(lngDistance / 2)

      val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))

      return Math.round(AVERAGE_RADIUS_OF_EARTH * c)
    }

    /*
   *将基站ID通过字典表转换成对应的经纬度
   */
    val mapdata = sdata.filter(x => x.split(",").length > 18).map(x => (x.split(",")(1) + "_" + x.split(",")(2), x.split(",")(16) + "|" + x.split(",")(17))).collectAsMap
    val data_2 = user_sort.map { x =>
      val xx = x._2
      var yy = List[String]()
      for (i <- (0 until xx.size - 1)) {
        val temp = xx(i).split("\\|")
        if (temp.size > 2) {
          val location = mapdata.getOrElse(temp(temp.size - 1), "error")
          yy = yy :+ (temp(0) + "|" + temp(1) + "|" + location)
        }
      }
      (x._1, yy)
    }.filter(x => !x._2.toString.contains("error"))

    /*
   *将一个list中的经纬度转换成距离
   *这里的距离是相邻两个点之间的距离
   */
    def cal(xx: List[String]): List[String] = {
      var tmp = List[String]()
      tmp = tmp :+ xx(0) + "|0"
      for (i <- (1 until xx.size - 1)) {
        val start_lat = xx(i - 1).split("\\|")(2)
        val start_long = xx(i - 1).split("\\|")(3)
        val end_lat = xx(i).split("\\|")(2)
        val end_long = xx(i).split("\\|")(3)
        tmp = tmp :+ xx(i) + "|" + calc_distance(start_lat, start_long, end_lat, end_long)
      }
      tmp
    }

    /*
   *提取部分字段
   */
    val data_3 = data_2.map(x => (x._1, cal(x._2)))
    def list_cut(list: List[String]): List[String] = {
      var tmp = List[String]()
      for (i <- (0 until list.size)) {
        var tmp_t = List[String]()
        //tmp_t = tmp_t :+ list(i).split("\\|")(0)
        tmp_t = tmp_t :+ list(i).split("\\|")(2)
        tmp_t = tmp_t :+ list(i).split("\\|")(3)
        tmp_t = tmp_t :+ list(i).split("\\|")(4)
        tmp = tmp :+ tmp_t.mkString("|")
      }
      tmp
    }
    val data_4 = data_3.map(x => (x._1, list_cut(x._2)))

    //判断用户轨迹是否满足小于1KM的点是否大于80%
    def list_judge(list: List[String]): Boolean = {
      var count = 0
      for (i <- (0 until list.size)) {
        // var tmp = list(i).split("\\|")(2).toDouble
        val tmp = list(i).split("\\|")(4).toDouble
        if (tmp < 1.0)
          count = count + 1
      }
      if (count * 100 / list.size >= 80)
        return true
      else
        return false
    }

    //合并时间窗
    def list_trans(list: List[String]): List[String] = {
      var tmp = List[String]()
      for (i <- (0 until list.size)) {
        val tmp_s = list(i).split("\\|")(2).toDouble
        if (tmp_s < 1.0)
          tmp = tmp :+ list(i).drop(1)
      }
      return tmp
    }


    //
    val data_5 = data_4.filter(x => list_judge(x._2)).map(x => (x._1, list_trans(x._2).distinct)).filter(x => x._2.size >= 2)
    data_5.saveAsTextFile("lzy/Comm/20150611-01")
  }
}
