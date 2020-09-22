import java.sql.{Connection, DriverManager}

/** Custom sink to send the results to MySQL
  *
  *
  */
class JDBCSink(url: String, user: String, pwd: String) extends org.apache.spark.sql.ForeachWriter[org.apache.spark.sql.Row] {
    val driver = "com.mysql.jdbc.Driver"
    var conn: Connection = _

  /** Initializes the process
    *
    * @param partitionId
    * @param version
    * @return
    */
    def open(partitionId: Long, version: Long): Boolean = {
      Class.forName(driver)
      conn = DriverManager.getConnection(url,user,pwd)
      conn.setAutoCommit(false);
      System.out.println("got JDBCSink connection")

      true
    }



  /** Executes the process
    *
    * @param value a list with the plc parameters and values
    */
    def process(value: org.apache.spark.sql.Row): Unit = {
        val sendQuery = InsertByRow.insertParsing(value)
        System.out.println("Query value is: " + sendQuery)
        conn.createStatement.executeUpdate(sendQuery)
    }



  /** Ends the process
    *
    * @param errorOrNull a message error if there is an error or null if there is not an error
    */
    def close(errorOrNull: Throwable): Unit = {
      conn.commit()
      conn.close
      System.out.println("JDBCSink: Result of errorOrNull: " + errorOrNull)
    }
  }