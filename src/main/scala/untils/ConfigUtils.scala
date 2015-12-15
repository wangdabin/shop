package untils

/**
 * Created by wangdabin1216 on 15/11/2.
 */
import java.util.Properties
import scala.collection.JavaConversions.propertiesAsScalaMap
/*
 * this object is a toolbox,add some common def to here
 * */
object ConfigUtils {

  //get property's message
  def getConfig(path:String) :  scala.collection.mutable.Map[String,String] = {
    val prop = new Properties()
    val inputStream = getClass().getResourceAsStream(path)
    try{
      prop.load(inputStream)
      propertiesAsScalaMap(prop)
    }finally inputStream.close()
  }

  //do some test
  def main (args:Array[String]){
//    val configs = ConfigUtils.getConfig("/config/db.properties")
//    val url = configs.get("MYSQL_DB_URL")
//    val username = configs.get("MYSQL_DB_USERNAME")
//    val password = configs.get("MYSQL_DB_PASSWORD")
//    println(username)
//    println(password)
//    var set = Set[String]()
//    set+="1"
//    set.foreach(println(_))
   println((Math.random() * 3 +1).toInt)



  }
}