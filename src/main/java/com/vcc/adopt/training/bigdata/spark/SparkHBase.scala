package com.vcc.adopt.training.bigdata.spark

import com.vcc.adopt.config.ConfigPropertiesLoader
import com.vcc.adopt.utils.hbase.HBaseConnectionFactory
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Get, Put}
import org.apache.hadoop.hbase.client.{Get, Put}
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
import org.apache.hadoop.hbase.client.{Connection, Get, Result}
import scala.collection.mutable.ListBuffer
import org.apache.hadoop.hbase.filter.{BinaryComparator, CompareFilter, SingleColumnValueFilter}
import java.util.{Date, Locale}
import org.apache.hadoop.hbase.client.Scan
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

  // ######################################################### START #######################################################
  //  private def readHDFSThenPutToHBase(): Unit = {
  //    // Định nghĩa kiểu dữ liệu của mô hình log
  //    val schema = StructType(Seq(
  //      StructField("timeCreate", TimestampType, nullable = true),
  //      StructField("cookieCreate", TimestampType, nullable = true),
  //      StructField("browserCode", IntegerType, nullable = true),
  //      StructField("browserVer", StringType, nullable = true),
  //      StructField("osCode", IntegerType, nullable = true),
  //      StructField("osVer", StringType, nullable = true),
  //      StructField("ip", LongType, nullable = true),
  //      StructField("locId", IntegerType, nullable = true),
  //      StructField("domain", StringType, nullable = true),
  //      StructField("siteId", IntegerType, nullable = true),
  //      StructField("cId", IntegerType, nullable = true),
  //      StructField("path", StringType, nullable = true),
  //      StructField("referer", StringType, nullable = true),
  //      StructField("guid", LongType, nullable = true),
  //      StructField("flashVersion", StringType, nullable = true),
  //      StructField("jre", StringType, nullable = true),
  //      StructField("sr", StringType, nullable = true),
  //      StructField("sc", StringType, nullable = true),
  //      StructField("geographic", IntegerType, nullable = true),
  //      StructField("category", IntegerType, nullable = true)
  //    ))
  //    // Khởi tạo Spark Session
  //    val spark = SparkSession.builder()
  //      .appName("ReadDatWriteParquet")
  //      .master("local[*]")
  //      .getOrCreate()
  //
  //    // Đường dẫn đến tập tin dat
  //    val datFilePath = "hdfs://namenode:9000/datalog/sampledata/mergedata.dat"
  //
  //    // Đọc tập tin dat với schema đã định nghĩa
  //    val datDataFrame = spark.read
  //      .option("delimiter", "\t")
  //      .schema(schema)
  //      .format("csv")
  //      .load(datFilePath)
  //
  //    // Hiển thị nội dung của DataFrame
  //    datDataFrame.show(100)
  //
  //    // Kết nối tới HBase
  //    val hbaseConnection = HBaseConnectionFactory.createConnection()
  //
  //    try {
  //      datDataFrame.foreachPartition(rows => {
  //        val hbaseConnection = HBaseConnectionFactory.createConnection()
  //        val table = hbaseConnection.getTable(TableName.valueOf("person", "person_info"))
  //
  //
  //        for (row <- rows) {
  //          val put = new Put(Bytes.toBytes(row.getAs[Long]("guid")))
  //
  //          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("timeCreate"), Bytes.toBytes(row.getAs[Timestamp]("timeCreate").toString))
  //          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("cookieCreate"), Bytes.toBytes(row.getAs[Timestamp]("cookieCreate").toString))
  //          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("browserCode"), Bytes.toBytes(row.getAs[Int]("browserCode")))
  //          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("browserVer"), Bytes.toBytes(row.getAs[String]("browserVer")))
  //          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("osCode"), Bytes.toBytes(row.getAs[Int]("osCode")))
  //          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("osVer"), Bytes.toBytes(row.getAs[String]("osVer")))
  //          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("ip"), Bytes.toBytes(row.getAs[Long]("ip")))
  //          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("locId"), Bytes.toBytes(row.getAs[Int]("locId")))
  //          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("domain"), Bytes.toBytes(row.getAs[String]("domain")))
  //          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("siteId"), Bytes.toBytes(row.getAs[Int]("siteId")))
  //          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("cId"), Bytes.toBytes(row.getAs[Int]("cId")))
  //          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("path"), Bytes.toBytes(row.getAs[String]("path")))
  //          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("referer"), Bytes.toBytes(row.getAs[String]("referer")))
  //          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("guid"), Bytes.toBytes(row.getAs[Long]("guid")))
  //          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("flashVersion"), Bytes.toBytes(row.getAs[String]("flashVersion")))
  //          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("jre"), Bytes.toBytes(row.getAs[String]("jre")))
  //          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("sr"), Bytes.toBytes(row.getAs[String]("sr")))
  //          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("sc"), Bytes.toBytes(row.getAs[String]("sc")))
  //          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("geographic"), Bytes.toBytes(row.getAs[Int]("geographic")))
  //          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("category"), Bytes.toBytes(row.getAs[Int]("category")))
  //
  //          table.put(put)
  //        }
  //
  //        table.close()
  //      })
  //    } finally {
  //      hbaseConnection.close()
  //    }
  //
  //    // Đóng kết nối tới HBase
  //    hbaseConnection.close()
  //
  //    // Đóng Spark Session
  //    spark.stop()
  //  }


  // ######################################################### END #######################################################


  private def createDataFrameAndPutToHDFS(): Unit = {
    println(s"----- Make person info dataframe then write to parquet at ${personInfoLogPath} ----")

    // tạo person-info dataframe và lưu vào HDFS
    val data = Seq(
      Row(1L, "Alice", 25),
      Row(2L, "Bob", 30),
      Row(3L, "Charlie", 22),
      Row(4L, "Yorn", 22)
    )
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

    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    df.show()
    df.write
      .mode("overwrite") // nếu file này đã tồn tại trước đó, sẽ ghi đè
      .parquet(personInfoLogPath)

    // tạo person-id-list và lưu vào HDFS
    df.select("personId")
      .write
      .mode("overwrite")
      .parquet(personIdListLogPath)
  }

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
          val browserVer = row.getAs[String]("browserVer")
          val osCode = row.getAs[Int]("osCode")
          val osVer = Option(row.getAs[String]("osVer")).getOrElse(" ")
          val ip = row.getAs[Long]("ip")
          val locId = row.getAs[Int]("locId")
          val domain = row.getAs[String]("domain")
          val siteId = row.getAs[Int]("siteId")
          val cId = row.getAs[Int]("cId")
          val path = row.getAs[String]("path")
          val referer = row.getAs[String]("referer")
          val guid = row.getAs[Long]("guid")
          val flashVersion = row.getAs[String]("flashVersion")
          val jre = row.getAs[String]("jre")
          val sr = row.getAs[String]("sr")
          val sc = row.getAs[String]("sc")
          val geographic = row.getAs[Int]("geographic")
          val category = row.getAs[Int]("category")

          val put = new Put(Bytes.toBytes(timeCreate))
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

  def getUrlVisitedByGuid(guid: Long, dateString: String): List[String] = {
    val connection = HBaseHelper.getConnection
    val table = hbaseConnection.getTable(TableName.valueOf("bai4", "pageviewlog"))
    val resultScanner: ResultScanner = table.getScanner(scan)
    val urls = ListBuffer[String]()
    try {
      val startRow = guid.toString + "_" + date.toString
      val stopRow = guid.toString + "_" + date.toString + "|"
      val scan = new Scan(Bytes.toBytes(startRow), Bytes.toBytes(stopRow))

      // Thực hiện quét dữ liệu từ bảng HBase
      val scanner = table.getScanner(scan)

      // Liệt kê các URL đã truy cập trong ngày của GUID
      scanner.forEach(result => {
        val path = Bytes.toString(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("path")))
        println(path)
      }
    } finally {
      resultScanner.close()
      table.close()
      // Không đóng kết nối ở đây để tái sử dụng lại kết nối
    }
    urls.toList
  }

  def datalogEx(): Unit = {
    // Khởi tạo SparkSession
    val spark = SparkSession.builder()
      .appName("ParquetKMeansProcessing")
      .getOrCreate()
    // Định nghĩa schema cho file Parquet
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

    // Đọc dữ liệu từ file Parquet
    //    val df = spark.read.parquet("hdfs://namenode:9000/datalog/sampledata/dataparquet/parquetmergefile.parquet/part-00000-49f07a1e-d9ff-4a22-8038-c3beb6031d70-c000.snappy.parquet")
    // Đường dẫn tới file Parquet
    val parquetFile = "hdfs://namenode:9000/datalog/sampledata/dataparquet/parquetmergefile.parquet/part-00000-49f07a1e-d9ff-4a22-8038-c3beb6031d70-c000.snappy.parquet"

    // Đọc dữ liệu từ file Parquet với schema đã định nghĩa
    val df: DataFrame = spark.read.schema(schema).parquet(datalog)

    // Hiển thị schema của DataFrame để xác định các trường dữ liệu
    df.printSchema()

    // 3.1. Lấy url đã truy cập nhiều nhất trong ngày của mỗi guid
    val dfWithDate = df.withColumn("timeCreate", to_date(col("timeCreate")))
    //    val urlCountPerGuid = df.groupBy("guid", "timeCreate", "path").count()
    val urlCountPerGuid = dfWithDate.groupBy("guid", "timeCreate", "path").agg(count("*").alias("access_count"))
    val windowSpec = Window.partitionBy("guid", "timeCreate").orderBy(col("access_count").desc)
    val topUrlPerGuid = urlCountPerGuid.withColumn("rank", row_number().over(windowSpec)).where(col("rank") === 1)
    topUrlPerGuid.show()

    // 3.2. Các IP được sử dụng bởi nhiều guid nhất
    val ipCountPerGuid = df.groupBy("ip").agg(countDistinct("guid").alias("guid_count"))
    val topIPs = ipCountPerGuid.orderBy(col("guid_count").desc).limit(1000)
    topIPs.show()

    // 3.3. Lấy top 100 các domain được truy cập nhiều nhất
    val topDomains = df.groupBy("domain").count().orderBy(col("count").desc).limit(100)
    topDomains.show()

    // 3.4. Lấy top 10 các LocId có số lượng IP không trùng nhiều nhất
    val topLocIds = df.groupBy("locId").agg(countDistinct("ip").alias("unique_ip_count")).orderBy(col("unique_ip_count").desc).limit(10)
    topLocIds.show()

    // 3.5. Tìm trình duyệt phổ biến nhất trong mỗi hệ điều hành (osCode và browserCode)
    val popularBrowserByOS = df.groupBy("osCode", "browserCode").count()
    val windowSpecOS = Window.partitionBy("osCode").orderBy(col("count").desc)
    val topBrowserByOS = popularBrowserByOS.withColumn("rank", row_number().over(windowSpecOS)).where(col("rank") === 1).drop("count")
    topBrowserByOS.show()

    // 3.6. Lọc các dữ liệu có timeCreate nhiều hơn cookieCreate 10 phút, và chỉ lấy field guid, domain, path, timecreate và lưu lại thành file result.dat định dạng text và tải xuống.
    val filteredData = df.filter(col("timeCreate").cast("long") > col("cookieCreate").cast("long") + lit(600000))
      .select("guid", "domain", "path", "timeCreate")

    val stringTypedData = filteredData.selectExpr(
      "CAST(guid AS STRING) AS guid",
      "CAST(domain AS STRING) AS domain",
      "CAST(path AS STRING) AS path",
      "CAST(timeCreate AS STRING) AS timeCreate"
    )
    val tabSeparatedData = stringTypedData.withColumn("concatenated",
      concat_ws("\t", col("guid"), col("domain"), col("path"), col("timeCreate"))
    ).select("concatenated")
    tabSeparatedData.write.text(outputFilePath)
    //    tabSeparatedData.show()
    // Dừng SparkSession
    spark.stop()
  }

  //  private def kmeanEx(k: Int): Unit = {
  //    println("Start")
  //    val data: DataFrame = spark.read.text(output4)
  //      .toDF("data")
  //
  //    // Chuyển đổi dữ liệu từ cột 'data' sang cột 'x' và 'y'
  //    val parsedData = data.selectExpr("cast(split(data, ',')[0] as double) as x", "cast(split(data, ',')[1] as double) as y")
  //
  //    // Hiển thị dữ liệu
  //    //    parsedData.show()
  //
  //    // Tạo một đối tượng KMeans
  //    val assembler = new VectorAssembler()
  //      .setInputCols(Array("x", "y"))
  //      .setOutputCol("features")
  //
  //    val dataWithFeatures = assembler.transform(parsedData)
  //    //    dataWithFeatures.show()
  //    // ####
  //
  //
  //    val kmeans = new KMeans()
  //      .setK(k) // Số lượng cụm
  //      .setSeed(1L) // Seed để tái tạo kết quả
  //
  //    val model = kmeans.fit(dataWithFeatures)
  //
  //    val centroids = model.clusterCenters
  //    centroids.foreach(println)
  //
  //    val predictions = model.transform(dataWithFeatures)
  //    predictions.show(30)
  //  }

  def main(args: Array[String]): Unit = {
    val connection = ConnectionFactory.createConnection()
    //    createDataFrameAndPutToHDFS()
    readHDFSThenPutToHBase()
    //    readHBaseThenWriteToHDFS()
    //    datalogEx()
    //    kmeanEx(3)
    getUrlVisitedByGuid(6638696843075557544L, "2018-08-10 10:57:17")
  }
}
