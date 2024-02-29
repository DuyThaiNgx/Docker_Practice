package com.vcc.adopt.training.bigdata.spark

import com.vcc.adopt.utils.hbase.HBaseConnectionFactory
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Get, Put}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.hadoop.hbase.util.Bytes

import java.util
import java.sql.{Connection, DriverManager, ResultSet}


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
  var connect: Connection = null
  var connectTitle: Connection = null
  var resultSet: ResultSet = null
  //  def resultSetToDataFrame(resultSet: ResultSet): DataFrame = {
  //    import spark.implicits._
  //    val rows = Iterator.continually(resultSet).takeWhile(_.next()).map { row =>
  //      (row.getString("dept_no"), row.getInt("emp_no"),
  //        row.getString("first_name"), row.getString("last_name"),
  //        row.getDate("birth_date"), row.getDate("hire_date")
  //      )
  //    }
  //    val df = rows.toSeq.toDF("dept_no", "emp_no", "first_nam", "last_name", "birth_date", "hire_date")
  //    df
  //  }

  private def readMySqlThenPutToHBase(): Unit = {
    println("----- Read employees on mySql then put to table bai5:deptemp ----")
    var deptEmp: DataFrame = null
    try {
      // Load driver
      Class.forName(driver)

      // Tạo kết nối
      connection = DriverManager.getConnection(url, username, password)

      // Thực hiện truy vấn
      val statement = connection.createStatement()
      val query = "SELECT concat(de.dept_no,\"_\", de.emp_no) as dept_emp, de.from_date as de_from_date, " +
        "de.to_date as de_to_date, e.emp_no, e.birth_date, e.first_name, e.last_name, e.gender, e.hire_date, d.dept_no, " +
        "d.dept_name, dm.from_date as dm_from_date, dm.to_date as dm_to_date FROM dept_emp de\n" +
        "left join employees e on de.emp_no = e.emp_no\nleft join departments d on de.dept_no = d.dept_no\n" +
        "left join dept_manager dm on de.dept_no = dm.dept_no and de.emp_no = dm.emp_no;"
      resultSet = statement.executeQuery(query)

      deptEmp = {
        import spark.implicits._
        val rows = Iterator.continually(resultSet).takeWhile(_.next()).map { row =>
          (row.getString("dept_emp"),
            row.getString("de_from_date"),
            row.getString("de_to_date"),
            row.getInt("emp_no"),
            row.getString("birth_date"),
            row.getString("first_name"),
            row.getString("last_name"),
            row.getString("gender"),
            row.getString("hire_date"),
            row.getString("dept_no"),
            row.getString("dept_name"),
            row.getString("dm_from_date"),
            row.getString("dm_to_date")
          )
        }
        val df = rows.toSeq.toDF("dept_emp", "de_from_date", "de_to_date", "emp_no", "birth_date", "first_name", "last_name", "gender", "hire_date", "dept_no", "dept_name", "dm_from_date", "dm_to_date")
        df
      }

    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      // Đóng kết nối
      if (resultSet != null) resultSet.close()
      if (connection != null) connection.close()
    }
    deptEmp = deptEmp
      .withColumn("country", lit("US"))
      .repartition(5)

    val batchPutSize = 100

    deptEmp.foreachPartition((rows: Iterator[Row]) => {
      // tạo connection hbase buộc phải tạo bên trong mỗi partition (không được tạo bên ngoài). Tối ưu hơn sẽ dùng connectionPool để reuse lại connection trên các worker
      val hbaseConnection = HBaseConnectionFactory.createConnection()
      try {
        val table = hbaseConnection.getTable(TableName.valueOf("bai5", "dept_empt"))
        val puts = new util.ArrayList[Put]()
        for (row <- rows) {
          val dept_emp = row.getAs[String]("dept_emp")
          val de_from_date = row.getAs[String]("de_from_date")
          val de_to_date = row.getAs[String]("de_to_date")
          val emp_no = row.getAs[Int]("emp_no")
          val birth_date = row.getAs[String]("birth_date")
          val first_name = row.getAs[String]("first_name")
          val last_name = row.getAs[String]("last_name")
          val gender = row.getAs[String]("gender")
          val hire_date = row.getAs[String]("hire_date")
          val dept_no = row.getAs[String]("dept_no")
          val dept_name = row.getAs[String]("dept_name")
          val dm_from_date = row.getAs[String]("dm_from_date")
          val dm_to_date = row.getAs[String]("dm_to_date")


          val put = new Put(Bytes.toBytes(dept_emp))
          put.addColumn(Bytes.toBytes("cf_infor"), Bytes.toBytes("de_from_date"), Bytes.toBytes(de_from_date))
          put.addColumn(Bytes.toBytes("cf_infor"), Bytes.toBytes("de_to_date"), Bytes.toBytes(de_to_date))
          put.addColumn(Bytes.toBytes("cf_employees"), Bytes.toBytes("emp_no"), Bytes.toBytes(emp_no))
          put.addColumn(Bytes.toBytes("cf_employees"), Bytes.toBytes("birth_date"), Bytes.toBytes(birth_date))
          put.addColumn(Bytes.toBytes("cf_employees"), Bytes.toBytes("first_name"), Bytes.toBytes(first_name))
          put.addColumn(Bytes.toBytes("cf_employees"), Bytes.toBytes("last_name"), Bytes.toBytes(last_name))
          put.addColumn(Bytes.toBytes("cf_employees"), Bytes.toBytes("gender"), Bytes.toBytes(gender))
          put.addColumn(Bytes.toBytes("cf_employees"), Bytes.toBytes("hire_date"), Bytes.toBytes(hire_date))
          put.addColumn(Bytes.toBytes("cf_department"), Bytes.toBytes("dept_no"), Bytes.toBytes(dept_no))
          put.addColumn(Bytes.toBytes("cf_department"), Bytes.toBytes("dept_name"), Bytes.toBytes(dept_name))
          if (dm_from_date != null) {
            put.addColumn(Bytes.toBytes("cf_manager"), Bytes.toBytes("dm_from_date"), Bytes.toBytes(dm_from_date))
          }
          if (dm_to_date != null) {
            put.addColumn(Bytes.toBytes("cf_manager"), Bytes.toBytes("dm_to_date"), Bytes.toBytes(dm_to_date))
          }
          puts.add(put)
          if (puts.size > batchPutSize) {
            table.put(puts)
            puts.clear()
          }
        }
        if (puts.size() > 0) { // đẩy nốt phần còn lại
          table.put(puts)
        }
      } finally {
        //        hbaseConnection.close()
      }
    })
  }


  private def readMySqlSalaries(): Unit = {
    println("----- Read employees on salaries ----")
    var salaries: DataFrame = null

    // Load driver
    Class.forName(driver)

    // Tạo kết nối
    connect = DriverManager.getConnection(url, username, password)

    // Thực hiện truy vấn
    val countQuerry = "SELECT COUNT(*) AS row_count FROM salaries"
    val queryStatement = connect.createStatement()
    val resultSetRow = queryStatement.executeQuery(countQuerry)
    resultSetRow.next()
    val rowNumber = resultSetRow.getInt("row_count")
    val sizeQuery = 300000
    val partitions = math.ceil(rowNumber.toDouble / sizeQuery).toInt
    for (i <- 0 until partitions) {
      val offset = i * sizeQuery // Offset cho mỗi phần
      val limit = sizeQuery // Số lượng dòng dữ liệu trong mỗi phần
      try {
        connection = DriverManager.getConnection(url, username, password)
        // Thực hiện truy vấn SQL cho phần hiện tại
        val query = "SELECT concat(s.emp_no, \"_\", s.from_date) as row_key, s.from_date, s.to_date, s.salary, " +
          "s.emp_no FROM salaries s LIMIT " + limit + " OFFSET " + offset
        val statement = connection.createStatement()
        val resultSet = statement.executeQuery(query)
        salaries = {
          import spark.implicits._
          val rows = Iterator.continually(resultSet).takeWhile(_.next()).map { row =>
            (row.getString("row_key"),
              row.getInt("emp_no"),
              row.getString("salary"),
              row.getString("from_date"),
              row.getString("to_date")
            )
          }
          val df = rows.toSeq.toDF("row_key", "emp_no", "salary", "from_date", "to_date")
          df
        }

      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        // Đóng kết nối
        if (resultSet != null) resultSet.close()
        if (connection != null) connection.close()
      }
      salaries = salaries
        .withColumn("country", lit("US"))
        .repartition(5)

      val batchPutSize = 100

      salaries.foreachPartition((rows: Iterator[Row]) => {
        // tạo connection hbase buộc phải tạo bên trong mỗi partition (không được tạo bên ngoài).
        // Tối ưu hơn sẽ dùng connectionPool để reuse lại connection trên các worker
        val hbaseConnection = HBaseConnectionFactory.createConnection()
        try {
          val table = hbaseConnection.getTable(TableName.valueOf("bai5", "salary"))
          val puts = new util.ArrayList[Put]()
          for (row <- rows) {
            val row_key = row.getAs[String]("row_key")
            val emp_no = row.getAs[Int]("emp_no")
            val salary = row.getAs[String]("salary")
            val from_date = row.getAs[String]("from_date")
            val to_date = row.getAs[String]("to_date")


            val put = new Put(Bytes.toBytes(row_key))
            put.addColumn(Bytes.toBytes("cf_infor"), Bytes.toBytes("emp_no"), Bytes.toBytes(emp_no))
            put.addColumn(Bytes.toBytes("cf_infor"), Bytes.toBytes("from_date"), Bytes.toBytes(from_date))
            put.addColumn(Bytes.toBytes("cf_infor"), Bytes.toBytes("to_date"), Bytes.toBytes(to_date))
            put.addColumn(Bytes.toBytes("cf_infor"), Bytes.toBytes("salary"), Bytes.toBytes(salary))
            puts.add(put)
            if (puts.size > batchPutSize) {
              table.put(puts)
              puts.clear()
            }
          }
          if (puts.size() > 0) { // đẩy nốt phần còn lại
            table.put(puts)
          }
        } finally {
          //        hbaseConnection.close()
        }
      })
    }
  }

  private def readMySqlTitles(): Unit = {
    println("----- Read employees on salaries ----")
    var titles: DataFrame = null

    // Load driver
    Class.forName(driver)

    // Tạo kết nối
    connectTitle = DriverManager.getConnection(url, username, password)

    // Thực hiện truy vấn
    val countQuerry = "SELECT COUNT(*) AS row_count FROM titles"
    val queryStatement = connectTitle.createStatement()
    val resultSetRow = queryStatement.executeQuery(countQuerry)
    resultSetRow.next()
    val rowNumber = resultSetRow.getInt("row_count")
    val sizeQuery = 300000
    val partitions = math.ceil(rowNumber.toDouble / sizeQuery).toInt
    for (i <- 0 until partitions) {
      try {
        connectTitle = DriverManager.getConnection(url, username, password)
        // Thực hiện truy vấn SQL cho phần hiện tại
        val query = "Select concat(t.emp_no, \"_\", t.from_date) as row_key, t.from_date, t.to_date, " +
          "t.title, t.emp_no from titles as t;"

        val statement = connectTitle.createStatement()
        val resultSet = statement.executeQuery(query)
        titles = {
          import spark.implicits._
          val rows = Iterator.continually(resultSet).takeWhile(_.next()).map { row =>
            (row.getString("row_key"),
              row.getInt("emp_no"),
              row.getString("title"),
              row.getString("from_date"),
              row.getString("to_date")
            )
          }
          val df = rows.toSeq.toDF("row_key", "emp_no", "title", "from_date", "to_date")
          df
        }

      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        // Đóng kết nối
        if (resultSet != null) resultSet.close()
        if (connectTitle != null) connectTitle.close()
      }
      titles = titles
        .withColumn("country", lit("US"))
        .repartition(5)

      val batchPutSize = 100

      titles.foreachPartition((rows: Iterator[Row]) => {
        // tạo connection hbase buộc phải tạo bên trong mỗi partition (không được tạo bên ngoài).
        // Tối ưu hơn sẽ dùng connectionPool để reuse lại connection trên các worker
        val hbaseConnection = HBaseConnectionFactory.createConnection()
        try {
          val table = hbaseConnection.getTable(TableName.valueOf("bai5", "title"))
          val puts = new util.ArrayList[Put]()
          for (row <- rows) {
            val row_key = row.getAs[String]("row_key")
            val emp_no = row.getAs[Int]("emp_no")
            val salary = row.getAs[String]("title")
            val from_date = row.getAs[String]("from_date")
            val to_date = row.getAs[String]("to_date")


            val put = new Put(Bytes.toBytes(row_key))
            put.addColumn(Bytes.toBytes("cf_infor"), Bytes.toBytes("emp_no"), Bytes.toBytes(emp_no))
            put.addColumn(Bytes.toBytes("cf_infor"), Bytes.toBytes("from_date"), Bytes.toBytes(from_date))
            put.addColumn(Bytes.toBytes("cf_infor"), Bytes.toBytes("to_date"), Bytes.toBytes(to_date))
            put.addColumn(Bytes.toBytes("cf_infor"), Bytes.toBytes("title"), Bytes.toBytes(salary))
            puts.add(put)
            if (puts.size > batchPutSize) {
              table.put(puts)
              puts.clear()
            }
          }
          if (puts.size() > 0) { // đẩy nốt phần còn lại
            table.put(puts)
          }
        } finally {
          //        hbaseConnection.close()
        }
      })
    }

  }

  private def readMySqlEx1(dept: String): Unit = {
    println("----- Lấy được danh sách, nhân viên & quản lý của 1 phòng ban cần truy vấn ----")
    var employees_dept: DataFrame = null
    // Load driver
    Class.forName(driver)
    var connectionEX1: Connection = null

    // Tạo kết nối
    connectionEX1 = DriverManager.getConnection(url, username, password)
    val statement = connectionEX1.createStatement()
    val query = "SELECT emp_no from employees;"
    resultSet = statement.executeQuery(query)
    try {
      employees_dept = {
        import spark.implicits._
        val rows = Iterator.continually(resultSet).takeWhile(_.next()).map { row =>
          (row.getString("emp_no"))
        }
        val df = rows.toSeq.toDF("emp_no")
        df
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      // Đóng kết nối
      if (resultSet != null) resultSet.close()
      if (connectionEX1 != null) connectionEX1.close()
    }
    import spark.implicits._
    val empListDF = employees_dept
      .repartition(5)
      .mapPartitions((rows: Iterator[Row]) => {
        val hbaseConnection = HBaseConnectionFactory.createConnection()
        val table = hbaseConnection.getTable(TableName.valueOf("bai5", "dept_empt"))
        try {
          rows.map(row => {
            val get = new Get(Bytes.toBytes(dept + "_" + row.getAs[String]("emp_no")))
            get.addColumn(Bytes.toBytes("cf_employees"), Bytes.toBytes("emp_no"))
            get.addColumn(Bytes.toBytes("cf_employees"), Bytes.toBytes("birth_date"))
            get.addColumn(Bytes.toBytes("cf_employees"), Bytes.toBytes("first_name"))
            get.addColumn(Bytes.toBytes("cf_employees"), Bytes.toBytes("last_name"))
            get.addColumn(Bytes.toBytes("cf_employees"), Bytes.toBytes("gender"))
            get.addColumn(Bytes.toBytes("cf_employees"), Bytes.toBytes("hire_date"))
            (
              Bytes.toInt(table.get(get).getValue(Bytes.toBytes("cf_employees"), Bytes.toBytes("emp_no"))),
              Bytes.toString(table.get(get).getValue(Bytes.toBytes("cf_employees"), Bytes.toBytes("birth_date"))),
              Bytes.toString(table.get(get).getValue(Bytes.toBytes("cf_employees"), Bytes.toBytes("first_name"))),
              Bytes.toString(table.get(get).getValue(Bytes.toBytes("cf_employees"), Bytes.toBytes("last_name"))),
              Bytes.toString(table.get(get).getValue(Bytes.toBytes("cf_employees"), Bytes.toBytes("gender"))),
              Bytes.toString(table.get(get).getValue(Bytes.toBytes("cf_employees"), Bytes.toBytes("hire_date"))),
            )
          })
        } finally {
          //          hbaseConnection.close()
        }
      }).toDF("emp_no", "birth_date", "first_name", "last_name", "gender", "hire_date")

    empListDF.persist()
    empListDF.show(empListDF.count().toInt)

    import spark.implicits._
    val managerListDF = employees_dept
      .repartition(5)
      .mapPartitions((rows: Iterator[Row]) => {
        val hbaseConnection = HBaseConnectionFactory.createConnection()
        val table = hbaseConnection.getTable(TableName.valueOf("bai5", "dept_empt"))
        try {
          rows.map(row => {
            val get = new Get(Bytes.toBytes(dept + "_" + row.getAs[String]("emp_no")))
            get.addColumn(Bytes.toBytes("cf_manager"), Bytes.toBytes("dm_from_date"))
            get.addColumn(Bytes.toBytes("cf_employees"), Bytes.toBytes("emp_no"))
            get.addColumn(Bytes.toBytes("cf_employees"), Bytes.toBytes("birth_date"))
            get.addColumn(Bytes.toBytes("cf_employees"), Bytes.toBytes("first_name"))
            get.addColumn(Bytes.toBytes("cf_employees"), Bytes.toBytes("last_name"))
            get.addColumn(Bytes.toBytes("cf_employees"), Bytes.toBytes("gender"))
            get.addColumn(Bytes.toBytes("cf_employees"), Bytes.toBytes("hire_date"))
            if (table.get(get).getValue(Bytes.toBytes("cf_employees"), Bytes.toBytes("emp_no")) != null) {
              (
                Bytes.toInt(table.get(get).getValue(Bytes.toBytes("cf_employees"), Bytes.toBytes("emp_no"))),
                Bytes.toString(table.get(get).getValue(Bytes.toBytes("cf_employees"), Bytes.toBytes("birth_date"))),
                Bytes.toString(table.get(get).getValue(Bytes.toBytes("cf_employees"), Bytes.toBytes("first_name"))),
                Bytes.toString(table.get(get).getValue(Bytes.toBytes("cf_employees"), Bytes.toBytes("last_name"))),
                Bytes.toString(table.get(get).getValue(Bytes.toBytes("cf_employees"), Bytes.toBytes("gender"))),
                Bytes.toString(table.get(get).getValue(Bytes.toBytes("cf_employees"), Bytes.toBytes("hire_date")))
              )
            }
            else {
              None
            }
          })
        } finally {
          //          hbaseConnection.close()
        }
      }).toDF("emp_no", "birth_date", "first_name", "last_name", "gender", "hire_date")

    managerListDF.persist()
    managerListDF.show(managerListDF.count().toInt)
  }

  def main(args: Array[String]): Unit = {
    //    readMySqlThenPutToHBase()
    //    readMySqlSalaries()
//    readMySqlTitles()
    readMySqlEx1("d001")
  }
}






