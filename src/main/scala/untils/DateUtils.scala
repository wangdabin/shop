package untils

import java.text.SimpleDateFormat
import java.util.Date

/**
 * Created by wangdabin1216 on 15/11/11.
 */
object DateUtils {

  def toDate(s: String): Date = new SimpleDateFormat("yyyy-MM-dd").parse(s)

  def toString(s:Date):String  = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(s)

  def main(args: Array[String]) {
    println(new Date(1445656218))
  }
}
