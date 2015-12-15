package mlearn

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object ProductLensALS {
  def main(args: Array[String]) {
    //    if (args.length < 1) {
    //      //      System.err.println("Usage: <startTime> <stopTime> <top>  eg: 1991-10-31 1992-10-31 10")
    //      System.err.println("Usage: <top>  eg: 10")
    //      System.exit(1)
    //    }
    //屏蔽不必要的日志显示在终端上
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    //设置运行环境
    val sparkConf = new SparkConf().setAppName("ProductLensALS")
    .set("spark.default.parallelism","24")
    val sc = new SparkContext(sparkConf)

    //    val startTime = DateUtils.toDate(args(0)).getTime
    //    val endTime = DateUtils.toDate(args(1)).getTime

    //1.hive生成对应用户评分,并装载
    val hiveContext = new HiveContext(sc)
    import hiveContext.sql
    val dbName = "dm_recsys_model"
    sql("use " + dbName)
    val ratings_sql = "select * from score_weight_sum"
    val ratingsDf = sql(ratings_sql).cache()
    //得到对应的映射关系
    val userMap = ratingsDf.map(_.get(0)).distinct().zipWithUniqueId().map(x => {
      (x._1, x._2.toInt)
    })
    val productMap = ratingsDf.map(_.get(1)).distinct().zipWithUniqueId().map(x => {
      (x._1, x._2.toInt)
    })
    //得到对应的逆映射
    val userReversalMap = userMap.map(x => {
      (x._2, x._1)
    })
    val productReversalMap = productMap.map(x => {
      (x._2, x._1)
    })

    //用户和产品的uuid分别关联映射
    val ratringsTf = ratingsDf.map(x => {
      (x.get(0), x.get(1), x.get(2))
    }).map(x => {
      (x._1, (x._2, x._3))
    })
    val mid1 = ratringsTf.leftOuterJoin(userMap).map(x => {
      (x._2._1._1, (x._1, x._2._1._2, x._2._2.get))
    })
    val mid2 = mid1.leftOuterJoin(productMap).map(x => {
      (x._2._1._3, x._2._2.get, x._2._1._2)
    })



    //装载样本评分数据，随机按照概率生成key，Rating为值，即(Int，Rating)

    //    val arr = Array(1, 1, 1, 1, 1, 1, 1, 1, 6, 8) //概率分布
    //    val ratings = mid2.map { x => {
    //        val key = arr((Math.random() * arr.length).toInt)
    //        (key, Rating(x._1, x._2, x._3.toString.toDouble))
    //      }
    //      }
    val ratings = mid2.map { x => {
      Rating(x._1, x._2, x._3.toString.toDouble)
    }
    }



    //统计有用户数量和电影数量以及用户对电影的评分数目
    //    val numRatings = ratings.count()
    //    val numUsers = ratings.map(_._2.user).distinct().count()
    //    val numProducts = ratings.map(_._2.product).distinct().count()
    //    println("Got " + numRatings + " ratings from " + numUsers + " users " + numProducts + " Products")

    //将样本评分表以key值切分成3个部分，分别用于训练 (80%，并加入用户评分), 校验 (10%), and 测试 (10%)
    //该数据在计算过程中要多次应用到，所以cache到内存
    //    val numPartitions = 4
    //    val training = ratings.filter(x => x._1 < 6).values.repartition(numPartitions).persist()
    //    val validation = ratings.filter(x => x._1 >= 6 && x._1 < 8).values.repartition(numPartitions).persist()
    //    val test = ratings.filter(x => x._1 >= 8).values.persist()

    //    val numTraining = training.count()
    //    val numValidation = validation.count()
    //    val numTest = test.count()
    //    if (numTraining == 0 || numValidation == 0 || numTest == 0) {
    //      throw new RuntimeException("error:训练模型数据太少,无法进行训练,请检查数据量!")
    //    }

    //    println("Training: " + numTraining + " validation: " + numValidation + " test: " + numTest)


    //训练不同参数下的模型，并在校验集中验证，获取最佳参数下的模型
    //    val ranks = List(8, 12)
    //    val lambdas = List(0.1, 10.0)
    //    val numIters = List(10, 20)
    //    var bestModel: Option[MatrixFactorizationModel] = None
    //    var bestValidationRmse = Double.MaxValue
    //    var bestRank = 0
    //    var bestLambda = -1.0
    //    var bestNumIter = -1
    //    The best model was trained with rank = 12 and lambda = 0.1, and numIter = 20, and its RMSE on the test set is 0.32297302449565274

    //    for (rank <- ranks; lambda <- lambdas; numIter <- numIters) {
    //显示计算
    //      val model = ALS.train(training, rank, numIter, lambda)
    val model = ALS.train(ratings, 12, 20, 0.1)
    //隐式计算
    //      val model = ALS.trainImplicit(training, rank, numIter, lambda,0.01)
    //      val validationRmse = computeRmse(model, validation, numValidation)
    //      println("RMSE(validation) = " + validationRmse + " for the model trained with rank = "
    //        + rank + ",lambda = " + lambda + ",and numIter = " + numIter + ".")

    //      if (validationRmse < bestValidationRmse) {
    //        bestModel = Some(model)
    //        bestValidationRmse = validationRmse
    //        bestRank = rank
    //        bestLambda = lambda
    //        bestNumIter = numIter
    //      }
    //    }

    //用最佳模型预测测试集的评分，并计算和实际评分之间的均方根误差（RMSE）
    //    val testRmse = computeRmse(bestModel.get, test, numTest)
    //    println("The best model was trained with rank = " + bestRank + " and lambda = " + bestLambda
    //      + ", and numIter = " + bestNumIter + ", and its RMSE on the test set is " + testRmse + ".")

    //    //create a naive baseline and compare it with the best model
    //    val meanRating = training.union(validation).map(_.rating).mean
    //    val baselineRmse = math.sqrt(test.map(x => (meanRating - x.rating) * (meanRating - x.rating)).reduce(_ + _) / numTest)
    //    val improvement = (baselineRmse - testRmse) / baselineRmse * 100
    //    println("The best model improves the baseline by " + "%1.2f".format(improvement) + "%.")

    //推荐前十部最感兴趣的产品,没有剔除用户已经购买的产品
    //建立用户和产品的笛卡尔积,分析所有用户的情况
    //    val usersProducts = userMap.map(x => {
    //      ("A", x._2)
    //    }).join(productMap.map(x => {
    //      ("A", x._2)
    //    })).map(_._2)
    //统计多少个
    //    val top = args(0).toInt
    //    val recommendations = bestModel.get
    //      .predict(usersProducts)
    //      .groupBy(_.user)
    //      .flatMap(x => {
    //      x._2.toList.sortBy(-_.rating).take(top)

    //    })

    val productNums = productMap.count().toInt
    val userNums = userMap.count()
    println("产品总数:" + productNums + ";用户总数:" + userNums)
    //使用优化策略
//val recommendations = ExtMatrixFactorizationModelHelper.recommendProductsForUsers( model, productNums, 420000, StorageLevel.MEMORY_AND_DISK_SER )
   //不使用优化策略
    val recommendations = model.recommendProductsForUsers(productNums.toInt)
    val mid_result = recommendations.flatMap(x => {
      x._2
    }).map(x => {
      (x.user, (x.product, x.rating))
    }).leftOuterJoin(userReversalMap).map(x => {
      (x._2._1._1, (x._1, x._2._1._2, x._2._2.get))
    }).leftOuterJoin(productReversalMap).map(x => {
      (x._2._1._3.toString, x._2._2.get.toString, x._2._1._2)
    })

    val result = mid_result.map(x => {
      val user_id = x._1
      val product_id = x._2
      val score = x._3
      user_id + "\t" + product_id + "\t" + score
    })

    //TODO 这里要将对应的原有的目录删除,确保可以save
    overwriteTextFile("model_cf/als",result)
//    result.saveAsTextFile("model_cf/als")
    //    val ratrings_mid = recommendations.map(x => {
    //      (x.user, (x.product, x.rating))
    //    })
    //    val ratrings_mid1 = ratrings_mid.leftOuterJoin(userReversalMap).map(x => {
    //      (x._2._1._1, (x._1, x._2._1._2, x._2._2.get))
    //    })
    //    val ratrings_mid2 = ratrings_mid1.leftOuterJoin(productReversalMap).map(x => {
    //      (x._2._1._3.toString, x._2._2.get.toString, x._2._1._2)
    //    })
    //
    //    //UUID5.fromString(x._2._1._3.toString + x._2._2.get.toString)
    //    val ratings_mysql = ratrings_mid2.map(x => {
    //      val id = UUID5.fromString(x._1 + x._2)
    //      val user_id = x._1
    //      val product_id = x._2
    //      val score = x._3.toString.toDouble
    //      val create_time = createTime
    //      (id, user_id, product_id, score, create_time)
    //    })
    //    //导入到mysql中
    //    //1.将mysql中原有的数据
    //    ratings_mysql.foreachPartition(MySqlUtils.saveRddForALS)
    //    //2.删除旧的数据
    //    MySqlUtils.deleteDataForALS(createTime)
        sc.stop()
  }

  def deletePath(sc: SparkContext, path: String): Unit = {
    val hdfs = org.apache.hadoop.fs.FileSystem.get(sc.hadoopConfiguration)
    val hdfsPath = new org.apache.hadoop.fs.Path(path)
    if (hdfs.exists(hdfsPath))
      hdfs.delete(hdfsPath, true)
  }

  def overwriteTextFile[T](path: String, rdd: RDD[T]): Unit = {
    deletePath(rdd.context, path)
    rdd.saveAsTextFile(path)
  }
  /** 校验集预测数据和实际数据之间的均方根误差 **/
  def computeRmse(model: MatrixFactorizationModel, data: RDD[Rating], n: Long): Double = {
    val predictions: RDD[Rating] = model.predict((data.map(x => (x.user, x.product))))
    val predictionsAndRatings = predictions.map { x => ((x.user, x.product), x.rating) }
      .join(data.map(x => ((x.user, x.product), x.rating))).values
    math.sqrt(predictionsAndRatings.map(x => (x._1 - x._2) * (x._1 - x._2)).reduce(_ + _) / n)
  }
}