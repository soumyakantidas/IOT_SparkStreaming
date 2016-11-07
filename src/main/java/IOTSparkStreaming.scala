import com.datastax.spark.connector.SomeColumns
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{StructField, StructType}

//import org.apache.spark.sql.types.{StructField, StructType}
import com.datastax.spark.connector.streaming._
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * Created by soumyaka on 11/7/2016.
  */
object IOTSparkStreaming {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .set("spark.cassandra.connection.host", "DIN16000309")
      .setAppName("IOT")
      .setMaster("local[*]")

    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(1))

    Logger.getRootLogger().setLevel(Level.ERROR)

    val kafkaParams = Map("metadata.broker.list" -> "DIN16000309:9092")

    //topics is the set to which this Spark instance will listen.
    val topics = List("fitbit", "new-user-notification").toSet

    val kafkaOutputBrokers = "DIN16000309:9092"
    val kafkaOutputTopic = "test-output1"
    val windowSec = 20
    val keySpaceName = "iot"
    val tableName = "user_details"

    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics).map(_._2)

    val fitbitStream = lines.filter(_.split(",")(0) == "fitbit")
    val newUserStream = lines.filter(_.split(",")(0) == "new-user-notification")
      .map(line => {
        val array = line.split(",")
        val age = array(1).trim.toInt
        val gender = array(2).trim
        val category = array(3).trim
        val weight = array(4).trim.toDouble
        val height = array(5).trim.toDouble
        val bmi = array(6).trim.toDouble
        val bfp = array(7).trim.toDouble
        val bpCat = array(8).trim
        val bpSys = array(9).trim.toDouble
        val bpDia = array(10).trim.toDouble
        val userID = array(11).trim
        val deviceID = array(12).trim
        /*val updateRow = Seq(Row(userID, deviceID, age, bfp, bmi, bpCat, bpDia,
          bpSys, category, gender, height, weight))*/

        (userID, deviceID, age, bfp, bmi, bpCat, bpDia,
          bpSys, category, gender, height, weight)
        // updateUserTable(spark, updateRow)
      }).saveToCassandra(keySpaceName, tableName, SomeColumns("user_id", "device_id", "age", "bfp", "bmi", "bp_cat",
      "bp_dia", "bp_sys", "category", "gender", "height", "weight"))


    //newUserStream.print()

    //Start the application
    ssc.checkpoint("C:/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }

  def updateUserTable(spark: SparkSession, updateRow: Seq[Row]): Unit = {
    val updateRdd = spark.sparkContext.parallelize(updateRow)
    val tblStruct = new StructType(
      Array(StructField("user_id", StringType, nullable = false),
        StructField("device_id", StringType, nullable = false),
        StructField("age", IntegerType, nullable = false),
        StructField("bfp", DoubleType, nullable = false),
        StructField("bmi", DoubleType, nullable = false),
        StructField("bp_cat", StringType, nullable = false),
        StructField("bp_dia", DoubleType, nullable = false),
        StructField("bp_sys", DoubleType, nullable = false),
        StructField("category", StringType, nullable = false),
        StructField("gender", StringType, nullable = false),
        StructField("height", DoubleType, nullable = false),
        StructField("weight", DoubleType, nullable = false)
      )
    )

    val updateDf = spark.sqlContext.createDataFrame(updateRdd, tblStruct)

    updateDf.write.format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> "IOT", "table" -> "user_details"))
      .mode("append")
      .save()
  }

}
