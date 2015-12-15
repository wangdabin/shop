package data

/**
 * Created by wangdabin1216 on 15/12/4.
 */
object DataTest {
  def main(args: Array[String]) {


    //屏蔽不必要的日志显示在终端上

    //设置运行环境
//    val sparkConf = new SparkConf().setAppName("MovieLensALS").setMaster("local")
//    val sc = new SparkContext(sparkConf)

//    val mysql ="33010;34482;50890;35296;50430;50889;50653;50935;34939;35336;33015;35234;50809;51021;35113;35320;50471;50470;34956;50722"
//    val substr = "35298,50644,50933,50938,50774,50929,50939,50932,50583,50645,35315,50928,50673,50930,35305,51078,50586,50589,50549,50604,50581,50580,35325,50585,50936,50758,50588,50926,50931,50888,50927,50648,51086,35182,50725,50937"
//
//
//    val mysqldata = mysql.split(";").toList
//    println(mysqldata.size)

//    substr.split(",").toList.foreach(x =>{
//      if(mysqldata.contains(x))
//        println(x)
//
//    }
//    )


//    val result = sc.textFile("/Users/wangdabin1216/work/lenovo/sbtSpark/data/000000_0")

//    result.map(x =>{
//      val a = x.split("\001")
////      (a(1),"goods_codes like '%" +a(0) +"%'")
//      (a(1),a(0))
////      (a(1),a(0))
//    }).filter(x=>{
//      StringUtils.isNotBlank(x._2)
//    }).map(x =>{
//      (x._1,"goods_codes like '%" +x._2 +"%'")
//    }).distinct().reduceByKey( _ + " or "  + _ ).foreach(println)
////      ).reduceByKey( _ + ","  + _ ).foreach(println)

//    result.map(x =>{
//      (1,"goods_codes like '%" +x +"%'")
//    }).distinct().reduceByKey( _ + " or "  + _ ).foreach(println)



//    val arr = "a@b@c@d@a"
//    arr.split("@").distinct.foreach(println)
  }
}
