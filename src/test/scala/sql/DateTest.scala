package sql

import java.text.SimpleDateFormat
import java.util.Date

/**
 * Created by wangdabin1216 on 15/12/1.
 */
object DateTest {
  def main(args: Array[String]) {

    val date = new Date(1448939801382l);
    println(new SimpleDateFormat("yyyy-MM-dd").format(date))

  }

}
