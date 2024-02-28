package com.vcc.adopt.training.bigdata.spark
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import java.sql.{DriverManager, ResultSet, Connection}


object SparkHBase {
  val spark: SparkSession = SparkSession.builder().getOrCreate()
  spark.sparkContext.setLogLevel("WARN")
  spark.conf.set("spark.sql.debug.maxToStringFields", 10000)
  // Thông tin kết nối đến cơ sở dữ liệu MySQL
  val url = "jdbc:mysql://localhost:3306/employees"
  val driver = "com.mysql.cj.jdbc.Driver"
  val username = "root"
  val password = "23092002"
  var connection: Connection = null
  var resultSet: ResultSet = null
  def resultSetToDataFrame(resultSet: ResultSet): DataFrame = {
    import spark.implicits._
    val rows = Iterator.continually(resultSet).takeWhile(_.next()).map { row =>
      (row.getString("dept_no"), row.getInt("emp_no"))
    }
    val df = rows.toSeq.toDF("dept_no", "emp_no")
    df
  }

  private def readMySqlThenPutToHBase(): Unit = {
    println("----- Read employees on mySql then put to table bai5:deptemp ----")

    try {
      // Load driver
      Class.forName(driver)

      // Tạo kết nối
      connection = DriverManager.getConnection(url, username, password)

      // Thực hiện truy vấn
      val statement = connection.createStatement()
      val query = "SELECT * FROM dept_emp"
      resultSet = statement.executeQuery(query)

      val data = resultSetToDataFrame(resultSet)
      data.show()

    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      // Đóng kết nối
      if (resultSet != null) resultSet.close()
      if (connection != null) connection.close()
    }
  }

  def main(args: Array[String]): Unit = {
    readMySqlThenPutToHBase()
  }
}






