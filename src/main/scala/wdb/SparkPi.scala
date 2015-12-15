/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println

package wdb

import org.apache.spark.{SparkConf, SparkContext}

/** Computes an approximation to pi */
object SparkPi{

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Pi").setMaster("local")
    val sc = new SparkContext(conf)


    val data1 = sc.parallelize(Array((("A","B"),"2"),(("A","B"),"5"),(("A","B"),"1")))
    println(data1.partitions.size)

    val data2 = sc.parallelize(Array((("A","B"),"2"),(("A","B"),"5"),(("A","B"),"1")))
    println(data2.partitions.size)
    val data3 = data1.union(data2)
    println(data3.partitions.size)

    val data4 = data3.groupByKey()
    data4.foreachPartition(println)






//    val map = sc.parallelize(Array((("A","B"),"2"),(("A","B"),"5"),(("A","C"),"1")))
//
//
//    map.reduceByKey(_ + "@" + _).foreach(println(_))
//
//
//    println(StringUtils.isNotBlank(""))


//    val x = (1,"a_1.1@b_1.2")
//    val goods_codes = x._2.split("@").sortBy(-_.split("_")(1).toDouble).take(1).map(_.split("_")(0)).mkString(";")
//
//    println(goods_codes)
//    val filter = List(("A","B"),("C","D"))
//
//    val list = List(("A","B"),("C","D"),("E","F"))
//
//    list.filter(x => {
//      !filter.contains(x)
//    }).foreach(println(_))






//    map.sortBy(-_._2).take(111).foreach(println(_))

//    val bc = sc.broadcast(map)
//    println(bc.value.size)

//val data = sc.textFile("/Users/wangdabin1216/work/lenovo/sbtSpark/src/main/scala/wdb/test.data")
//    val ratings = data.map(_.split(',') match { case Array(user, item, rate) =>
//      Rating(user.toInt, item.toInt, rate.toDouble)
//    })
//
//
//
//
//    // Build the recommendation model using ALS
//    val rank = 10
//    val numIterations = 20
//    val model = ALS.train(ratings, rank, numIterations, 0.1)
//
//    // Evaluate the model on rating data
//    val usersProducts = ratings.map { case Rating(user, product, rate) =>
//      (user, product)
//    }
//
//
//    val recommendation = ExtMatrixFactorizationModelHelper.recommendProductsForUsers(model,1,420000,StorageLevel.MEMORY_AND_DISK_SER)
//
//
//    recommendation.foreach(println(_))
//    /**
//     *
//     *
//     * 1,1,4.997616535964756
//       1,2,-0.3473050105367741
//       2,2,3.9970335653727465
//       2,1,-0.4322613682109644
//           -0.4322613682109644
//     */
//    println(model.predict(2,1))
//    model.recommendProductsForUsers(10).foreach(x =>{
//      println(x._1)
//      x._2.foreach(x => {
//        println(x.user + "," +x.product + "," + x.rating)
//      })
//    })
//    val numRecommendations = 2
//    val recommendations = ExtMatrixFactorizationModelHelper.recommendProductsForUsers(model,numRecommendations,420000,StorageLevel.MEMORY_AND_DISK_SER)
//
//    recommendations.foreach(x => {
//      println(x._2.size)
//      println("-------------")
//    })

//    val path = "model_cf/als"
//    val hdfs = org.apache.hadoop.fs.FileSystem.get(sc.hadoopConfiguration)
//    val hdfsPath = new org.apache.hadoop.fs.Path(path)
//    if (hdfs.exists(hdfsPath)) {
//      println(hdfsPath + "---------------------------------------")
//      hdfs.delete(hdfsPath, true)
//    }
//
//
//
//    val ratesAndPreds = ratings.map { case Rating(user, product, rate) =>
//      ((user, product), rate)
//    }.join(predictions)




//    val slices = if (args.length > 0) args(0).toInt else 2
//    val n = math.min(100000L * slices, Int.MaxValue).toInt // avoid overflow
//    val count = spark.parallelize(1 until n, slices).map { i =>
//      val x = random * 2 - 1
//      val y = random * 2 - 1
//      if (x*x + y*y < 1) 1 else 0
//    }.reduce(_ + _)
//    println("Pi is roughly " + 4.0 * count / n)
//    spark.stop()
//    val lines = sc.textFile("/Users/wangdabin1216/IdeaProjects/sbtSpark/data/tongqin.txt")
//    val users = lines.map(x => {
//      val user = x.split("\001")(0)
//      (user,1)
//    })
//
//    users.countByKey().foreach(x => {
//      println(x._1)
//    })

//    var rdd1 = sc.makeRDD(101 to 105,2)
//    var rdd2 = sc.makeRDD(Seq("A","B","C","D","E"),2)
//
//    var test = rdd1.zip(rdd2)
//
////
//    val data2 = Array(101,102,103,104,105)
//    val disData2 = sc.parallelize(data2)
//
//    val data3 = Array("A","C","E","B","D")
//    val disData3 = sc.parallelize(data3)
//
//    val result = disData2.distinct().zipWithUniqueId();
//    result.foreach(println(_))
//    val result2 = disData3.distinct().zipWithUniqueId();
//    result2.foreach(println(_))
//    test.map(x => {
//      (x._1,(x._2,"" + x._1 + x._2 ))
//    }).leftOuterJoin(result).map(x =>{
//      (x._2._1._1,(x._1,x._2._1._2,x._2._2.get))
//    }).leftOuterJoin(result2).map(x =>{
//      (x._2._1._3,x._2._2.get,x._2._1._2)
//    }).foreach(println(_))



//
//    val mid4 = Array((1,1),(2,2),(3,3),(1,4),(1,5))
//
//    mid4.groupBy(_._1).map(x => {
//     println(x._2.toList.sortBy(_._2).map(_._2) mkString ",")
//    })


//
//    disData.intersection(disData1).foreach(println(_))
//      val rdd1 = sc.parallelize(Array(("A","1"),("B","1")))
//      val rdd2 = sc.parallelize(Array(("B","5")))
//      val result = rdd1.join(rdd2)
//      result.foreach(println(_))
//
//
//      val leftResult = rdd1.leftOuterJoin(rdd2)



//    lines.cache()
//    lines.count()
//    println("--------------------------")
//    lines.count()







//    val result = disData.union(disData1)
//    result.foreach(x => {
//      println(x)
//    })




//    val group = users.groupByKey()
//
//    group.foreach(x => {println(x._1)
//      println(x._2)})
//
//    val usersTotal = users.reduceByKey((x,y) => x+y).sortByKey().sortBy(x => x._2).map(x =>{
//      x._1 + "," + x._2
//    })
//    println(usersTotal.first())
//    val fUsers = usersTotal.reduce((x,y) => {
//      x.split(",")(0) + "--" + y.split(",")(0)
//    })
//    println(fUsers)

//      val broadcastAList = sc.broadcast(List("1","2","3"))
//
//      val result = sc.parallelize(List("4","5")).map(x => {broadcastAList.value ++ x} )
//
//
//      println(result.collect())


//     val error = sc.accumulator(0)
//     println(error)
//     lines.filter(x => {
//       val user = x.split("\001")(0)
//       if(user.startsWith("460000005")){
//          error += 1
//         false
//       }else{
//          true
//       }
//     }).count()
//
//
//
//
//
//
//    println(error.value)

//    println(welcome("wdb"))
//    println(welcome1("123"))
//
//    val list = List(2,4,5,6,7)
//    println(list.foldLeft(0){(a:Int,b:Int) => a + b})


//    lines.map((x:String)=>{
      //用法1
//    })
//    lines.map(x => {
//    用法2
//    })
//    lines.map(_) 用法3
//    val array = new Array[String](3)
//    array(0) = "This"
//    array(1) = "is"
//
//    array.foreach(println(_))
//
//
//
//    val set1 = Set[Int](1,2,3)
//    val set2 = Set[Int](2,3,4)

    //并集
//    println(set1 ++ set2)
//    println(set1 | set2)
//    println(set1 union set2)
//    //交集
//    println(set1 & set2)
//    println(set1 intersect set2)
//    //差集
//    println(set1 -- set2)
//    println(set1 &~ set2)
//    println(set1 diff set2)
//    //
//    val list1 = List[Int](1,2,3,4,5,1,2)
//    list1.distinct.toSet





//    val rdd1 = sc.parallelize(1 to 2,1)
//    val rdd2 = sc.parallelize(2 to 3,1)
////    rdd1.union(rdd2).foreach(println(_))//并集
//
//
////    val rdd3 = rdd1.intersection(rdd2)//交集
////    rdd3.foreach(println(_))
//
//    val rdd4 = rdd1.subtract(rdd2)
//    rdd4.foreach(println(_))











//    usersTotal.saveAsTextFile("/Users/wangdabin1216/IdeaProjects/sbtSpark/data/result.txt")
  }
//  def welcome(name:String):String = {
//    "hello world"
//  }
//  def welcome1(name:String) = {
//    "hello"
//  }
}

// scalastyle:on println
