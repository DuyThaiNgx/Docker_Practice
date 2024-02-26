package com.vcc.adopt.training.bigdata.spark

import com.vcc.adopt.config.ConfigPropertiesLoader
import com.vcc.adopt.utils.hbase.HBaseConnectionFactory
import org.apache.avro.LogicalTypes.date
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Get, Put, Result, ResultScanner, Scan}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable.ListBuffer
import org.apache.hadoop.hbase.filter.{BinaryComparator, CompareFilter, SingleColumnValueFilter}

import java.sql.Timestamp
import java.util.{Date, Locale}
import java.text.SimpleDateFormat
import java.util.Date
import java.util


object SparkHBase {

  val spark: SparkSession = SparkSession.builder().getOrCreate()
  spark.sparkContext.setLogLevel("WARN")
  private val personInfoLogPath = ConfigPropertiesLoader.getYamlConfig.getProperty("personInfoLogPath")
  private val personIdListLogPath = ConfigPropertiesLoader.getYamlConfig.getProperty("personIdListLogPath")
  private val ageAnalysisPath = ConfigPropertiesLoader.getYamlConfig.getProperty("ageAnalysisPath")
  private val test = ConfigPropertiesLoader.getYamlConfig.getProperty("test2")
  private val datalog = ConfigPropertiesLoader.getYamlConfig.getProperty("data_log")
  private val output4 = ConfigPropertiesLoader.getYamlConfig.getProperty("output4")
  private val outputFilePath = ConfigPropertiesLoader.getYamlConfig.getProperty("outputFilePath")

  val schema = StructType(Seq(
    StructField("timeCreate", TimestampType, nullable = true),
    StructField("cookieCreate", TimestampType, nullable = true),
    StructField("browserCode", IntegerType, nullable = true),
    StructField("browserVer", StringType, nullable = true),
    StructField("osCode", IntegerType, nullable = true),
    StructField("osVer", StringType, nullable = true),
    StructField("ip", LongType, nullable = true),
    StructField("locId", IntegerType, nullable = true),
    StructField("domain", StringType, nullable = true),
    StructField("siteId", IntegerType, nullable = true),
    StructField("cId", IntegerType, nullable = true),
    StructField("path", StringType, nullable = true),
    StructField("referer", StringType, nullable = true),
    StructField("guid", LongType, nullable = true),
    StructField("flashVersion", StringType, nullable = true),
    StructField("jre", StringType, nullable = true),
    StructField("sr", StringType, nullable = true),
    StructField("sc", StringType, nullable = true),
    StructField("geographic", IntegerType, nullable = true),
    StructField("category", IntegerType, nullable = true)
  ))

  private def readHDFSThenPutToHBase(): Unit = {
    println("----- Read pageViewLog.parquet on HDFS then put to table pageviewlog ----")
    var df: DataFrame = spark.read.schema(schema).parquet(datalog)
    df = df
      .withColumn("country", lit("US"))
      .repartition(5) // chia dataframe thành 5 phân vùng, mỗi phân vùng sẽ được chạy trên một worker (nếu không chia mặc định là 200)

    val batchPutSize = 100 // để đẩy dữ liệu vào hbase nhanh, thay vì đẩy lẻ tẻ từng dòng thì ta đẩy theo lô, như ví dụ là cứ 100 dòng sẽ đẩy 1ần
    df.foreachPartition((rows: Iterator[Row]) => {
      // tạo connection hbase buộc phải tạo bên trong mỗi partition (không được tạo bên ngoài). Tối ưu hơn sẽ dùng connectionPool để reuse lại connection trên các worker
      val hbaseConnection = HBaseConnectionFactory.createConnection()
      try {
        val table = hbaseConnection.getTable(TableName.valueOf("bai4", "pageviewlog"))
        val puts = new util.ArrayList[Put]()
        for (row <- rows) {
          val timeCreate = Option(row.getAs[java.sql.Timestamp]("timeCreate")).map(_.getTime).getOrElse(0L)
          val cookieCreate = Option(row.getAs[java.sql.Timestamp]("cookieCreate")).map(_.getTime).getOrElse(0L)
          val browserCode = row.getAs[Int]("browserCode")
          val browserVer = Option(row.getAs[String]("browserVer")).getOrElse("")
          val osCode = row.getAs[Int]("osCode")
          val osVer = Option(row.getAs[String]("osVer")).getOrElse("")
          val ip = row.getAs[Long]("ip")
          val locId = row.getAs[Int]("locId")
          val domain = Option(row.getAs[String]("domain")).getOrElse("")
          val siteId = row.getAs[Int]("siteId")
          val cId = row.getAs[Int]("cId")
          val path = Option(row.getAs[String]("path")).getOrElse("")
          val referer = Option(row.getAs[String]("referer")).getOrElse("")
          val guid = row.getAs[Long]("guid")
          val flashVersion = Option(row.getAs[String]("flashVersion")).getOrElse("")
          val jre = Option(row.getAs[String]("jre")).getOrElse("")
          val sr = Option(row.getAs[String]("sr")).getOrElse("")
          val sc = Option(row.getAs[String]("sc")).getOrElse("")
          val geographic = row.getAs[Int]("geographic")
          val category = row.getAs[Int]("category")

          val put = new Put(Bytes.toBytes(cookieCreate))
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("timeCreate"), Bytes.toBytes(timeCreate))
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("cookieCreate"), Bytes.toBytes(cookieCreate))
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("browserCode"), Bytes.toBytes(browserCode))
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("browserVer"), Bytes.toBytes(browserVer))
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("osCode"), Bytes.toBytes(osCode))
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("osVer"), Bytes.toBytes(osVer))
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("ip"), Bytes.toBytes(ip))
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("locId"), Bytes.toBytes(locId))
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("domain"), Bytes.toBytes(domain))
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("siteId"), Bytes.toBytes(siteId))
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("cId"), Bytes.toBytes(cId))
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("path"), Bytes.toBytes(path))
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("referer"), Bytes.toBytes(referer))
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("guid"), Bytes.toBytes(guid))
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("flashVersion"), Bytes.toBytes(flashVersion))
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("jre"), Bytes.toBytes(jre))
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("sr"), Bytes.toBytes(sr))
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("sc"), Bytes.toBytes(sc))
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("geographic"), Bytes.toBytes(geographic))
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("category"), Bytes.toBytes(category))

          puts.add(put)
          if (puts.size() > batchPutSize) {
            table.put(puts)
            puts.clear()
          }
          if (puts.size() > 0) {
            table.put(puts)
          }
        }
      } finally {
        hbaseConnection.close()
      }
    })
  }

  private def readHBaseThenWriteToHDFS(): Unit = {
    println("----- Read person:info table to dataframe then analysis and write result to HDFS ----")
    /**
     * thống kê độ tuổi từ danh sách person_id
     * Cách xử lý:
     *    1. lấy danh sách person_id cần thống kê ở personIdListLogPath
     *       2. từ danh sách person_id lấy độ tuổi của mỗi người ở bảng person:person-info ở HBase
     *       3. dùng các phép transform trên dataframe để tính thống kê
     *       4. kết quả lưu vào HDFS
     */

    val personIdDF = spark.read.parquet(personIdListLogPath)
    import spark.implicits._
    val personIdAndAgeDF = personIdDF
      .repartition(5)
      .mapPartitions((rows: Iterator[Row]) => {
        val hbaseConnection = HBaseConnectionFactory.createConnection()
        val table = hbaseConnection.getTable(TableName.valueOf("person", "person_info"))
        try {
          rows.map(row => {
            val get = new Get(Bytes.toBytes(row.getAs[Long]("personId")))
            get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("timeCreate"))
            get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("cookieCreate"))
            (row.getAs[Long]("timeCreate"), Bytes.toString(table.get(get).getValue(Bytes.toBytes("cf"), Bytes.toBytes("timeCrate"))))
            (row.getAs[Long]("cookieCrate"), Bytes.toString(table.get(get).getValue(Bytes.toBytes("cf"), Bytes.toBytes("cookieCrate"))))
          })
        } finally {
          //          hbaseConnection.close()
        }
      }).toDF("personId", "age")

    personIdAndAgeDF.persist()
    personIdAndAgeDF.show()

    val analysisDF = personIdAndAgeDF.groupBy("age").count()
    analysisDF.show()
    analysisDF.write.mode("overwrite").parquet(ageAnalysisPath)

    personIdAndAgeDF.unpersist()
  }
  // 4.1


  def main(args: Array[String]): Unit = {
    val connection = ConnectionFactory.createConnection()
    //    createDataFrameAndPutToHDFS()
    //        readHDFSThenPutToHBase()
    //    readHBaseThenWriteToHDFS()
    //    datalogEx()
    //    kmeanEx(3)
        getUrlVisitedByGuid(8133866058245435043L, Timestamp.valueOf("2018-08-10 09:56:18").getTime)

    //    getMostUsedIPsByGuid(8133866058245435043L)
    //    findLatestAccessTimeByGuid(8133866058245435043L)
    //    getGUIDByOsCodeAndBrowsecode(10, 16, 1533195266000L, 1533995266000L)
  }
}

