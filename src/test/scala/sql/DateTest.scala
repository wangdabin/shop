package sql

import org.paukov.combinatorics.Factory
import scala.collection.JavaConverters._
/**
 * Created by wangdabin1216 on 15/12/1.
 */
object DateTest {
  def main(args: Array[String]) {


    val initialVector = Factory.createVector(
      Array("A","B","A"))
    val result = Factory.createSimpleCombinationGenerator(initialVector, 2)
    result.generateAllObjects().asScala.toList.map {
      item =>
        item.asScala.toList.sorted
    }.foreach(println)


    println(doubleFormat(1.1111))
  }
  def doubleFormat(x:Double):Double = {
    (x*100).toInt/100d
  }

}
