package untils

/**
 * Created by wangdabin1216 on 15/11/12.
 */
object ConnectionFactory {

  val configs = ConfigUtils.getConfig("/config/db.properties")
  val driver = configs.get("MYSQL_DB_DRIVER_CLASS").get
  val url = configs.get("MYSQL_DB_URL").get
  val username = configs.get("MYSQL_DB_USERNAME").get
  val password = configs.get("MYSQL_DB_PASSWORD").get

  import java.sql.{DriverManager, Connection, SQLException}

  private var driverLoaded = false

  @throws(classOf[SQLException])
  private def loadDriver() {
    if (!driverLoaded) {
      Class.forName(driver).newInstance
      driverLoaded = true
    }
  }

  @throws(classOf[SQLException])
  def connect(): Connection = {
    this.synchronized { loadDriver }
    DriverManager.getConnection(url, username, password)
  }

  def main(args: Array[String]) {
    ConnectionFactory.connect()
  }
}
