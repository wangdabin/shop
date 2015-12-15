package com.wdb.data

import java.text.SimpleDateFormat
import java.util.Date

/**
 * Created by wangdabin1216 on 15/11/4.
 */
object Test1 {
  def main(args: Array[String]): Unit = {
    val startTime = toDate("1991-08-30").getTime
    val endTime = toDate("1991-10-31").getTime
    println(startTime)
    println(endTime)
  }
  def toDate(s: String): Date = new SimpleDateFormat("yyyy-MM-dd").parse(s)

}
