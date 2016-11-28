import java.util

import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * Created by rajsarka on 11/7/2016.
  */
object workingTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .set("spark.cassandra.connection.host", "DIN16000309")
      .setAppName("IOT")
      .setMaster("local[*]")

    val spark = SparkSession.builder()
      .config(conf)
      //      .config("spark.sql.warehouse.dir", "file:///C:/temp")
      .getOrCreate()
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(1))


    Logger.getRootLogger().setLevel(Level.ERROR)

    val kafkaParams = Map("metadata.broker.list" -> "DIN16000309:9092")

    //topics is the set to which this Spark instance will listen.
    val topics = List("fitbit", "new-user-notification", "sales").toSet

    val kafkaOutputBrokers = "DIN16000309:9092"
    val kafkaOutputTopic = "mapData"
    val keySpaceName = "iot"
    val tableName = "user_details"
    val tableNameUserHistory = "userhistory"

    val cassandraSQLRDD = sc.cassandraTable(keySpaceName, tableNameUserHistory)

    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics).map(_._2)

    val fitbitStream = lines.filter(_.split(",")(0) == "fitbit")

    /*    warningNotification(fitbitStream, kafkaOutputTopic = "warningNotification", kafkaOutputBrokers)
        userHistory(fitbitStream, keySpaceName, tableNameUserHistory)*/
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
        (userID, age, bfp, bmi, bpCat, bpDia,
          bpSys, category, deviceID, gender, height, weight)
        // updateUserTable(spark, updateRow)
      })
      .saveToCassandra(keySpaceName, tableName, SomeColumns("user_id", "age", "bfp", "bmi", "bp_cat",
        "bp_dia", "bp_sys", "category", "device_id", "gender", "height", "weight"))

    val userDetailsTableRDD = sc.cassandraTable("iot", "user_details").select("user_id",
      "category").map(line => {
      (line.getString("user_id"), line.getString("category"))
    })


    warningNotification(fitbitStream, kafkaOutputTopic = "warningNotification", kafkaOutputBrokers)
    userHistory(fitbitStream, keySpaceName, tableNameUserHistory)
    obtainActivityLevel(ssc, fitbitStream, kafkaOutputBrokers)

    /*    val otherthing = something.foreachRDD(rdd => {
          val data = rdd.joinWithCassandraTable("iot", "user_details")
          data.foreach(println)
        })*/


    val saleStream = lines.filter(_.split(",")(0) == "sales")
      .map(line => {
        (line.split(",")(1).trim, line.split(",")(2).trim.toInt)
      })
      .saveToCassandra(keySpaceName, tableName = "sales", SomeColumns("date", "count"))


    userLatLongTable(fitbitStream, keySpaceName)

    //mapData(fitbitStream, kafkaOutputTopic, kafkaOutputBrokers)


    ssc.checkpoint("./checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }

  def warningNotification(fitbitStream: DStream[String], kafkaOutputTopic: String, kafkaOutputBrokers: String): Unit = {
    val data = fitbitStream
      .map(line => {
        val array = line.split(",")
        val userID = array(2).trim
        val pulse = (array(5).trim.toDouble + 0.5).toInt
        val temp = array(6).trim.toDouble
        val age = array(7).trim.toInt
        val bpCat = array(8).trim
        val machineTimeStamp = array(9).trim

        val maxPulseLimit = {
          if (age < 40) 220 - age else 208 - 0.75 * age
        }

        val warning = {
          if (pulse >= 0.95 * maxPulseLimit) {
            if (List("HYP_1", "HYP_2", "HYP_CR").contains(bpCat)) "critical"
            else "simple"
          } else "no-use"
        }
        (userID, warning, machineTimeStamp)
      })
      .filter(_._2 != "no-use")

    data.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {

        val producer = new KafkaProducer[String, String](setupKafkaProducer(kafkaOutputBrokers))
        partition.foreach(record => {
          val data = record.toString
          val message = new ProducerRecord[String, String](kafkaOutputTopic, data)
          producer.send(message)

        })
        producer.close()
      })
    })
  }

  def obtainActivityLevel(ssc: StreamingContext, fitbitStream: DStream[String], kafkaOutputBrokers:
  String): Unit = {
    val kafkaOutputTopic = "user-activity-category"
    val userDetailTableRDD = ssc.cassandraTable("iot", "user_details")

    val data = fitbitStream
      .map(line => {
        val array = line.split(",")
        val userID = array(2).trim
        val pulse = (array(5).trim.toDouble + 0.5).toInt
        val age = array(7).trim.toInt
        val bpCat = array(8).trim
        val machineTimeStamp = array(9).trim

        val maxPulseLimit = {
          if (age < 40) 220 - age else 208 - 0.75 * age
        }

        val warning = {
          if (pulse >= 0.95 * maxPulseLimit) {
            if (List("HYP_1", "HYP_2", "HYP_CR").contains(bpCat)) "critical"
            else "simple"
          } else "no-use"
        }
        (userID, warning, machineTimeStamp)
      })
      .filter(_._2 != "no-use")
      .map(line => {
        val user_id = line._1
        val machineTimeStamp = line._3

        (user_id, machineTimeStamp)
      })
      .joinWithCassandraTable("iot", "user_details", SomeColumns("category"), joinColumns =
        SomeColumns("user_id"))
      .map(line => {
        (line._1._1, line._1._2, line._2.getString("category"))
      })







    data.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
        val producer = new KafkaProducer[String, String](setupKafkaProducer(kafkaOutputBrokers))
        partition.foreach(record => {
          val data = record.toString()
          val message = new ProducerRecord[String, String](kafkaOutputTopic, data)
          producer.send(message)

        })
        producer.close()
      })
    })

  }


  def userHistory(fitbitStream: DStream[String], keySpaceName: String, tableName: String): Unit = {
    fitbitStream
      .map(line => {
        val array = line.split(",")
        val dateTime = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(array(1))
          .getTime.toString
        val simulationDate = array(1).trim.split(" ")(0)
        val userID = array(2).trim
        val lat = array(3).trim
        val long = array(4).trim
        val pulse = array(5).trim.toDouble
        val temp = array(6).trim.toDouble
        (userID, simulationDate, dateTime, lat, long, pulse, temp)
      })
      .saveToCassandra(keySpaceName, tableName, SomeColumns("user_id", "date", "time", "lat", "long", "pulse", "temp"))
    /*      .foreachRDD(rdd => {
          rdd.foreachPartition(partition => {
            partition.foreach(record => {
              println(record.toString)
            })
          })
        })*/
  }

  def userLatLongTable(fitbitStream: DStream[String], keySpaceName: String): Unit = {
    fitbitStream
      .map(line => {
        val array = line.split(",")
        val userID = array(2).trim
        val lat = array(3).trim
        val long = array(4).trim
        (userID, lat, long)
      }).saveToCassandra(keySpaceName, tableName = "latest_location", SomeColumns("user_id", "lat", "long"))
  }

  def mapData(fitbitStream: DStream[String], kafkaOutputTopic: String, kafkaOutputBrokers: String): Unit = {
    val data = fitbitStream
      .map(line => {
        val array = line.split(",")
        val userID = array(2).trim
        val lat = array(3).trim
        val long = array(4).trim
        val pulse = (array(5).trim.toDouble + 0.5).toInt
        val temp = array(6).trim.toDouble
        (userID, lat, long, pulse, temp)
      })

    data.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {

        val producer = new KafkaProducer[String, String](setupKafkaProducer(kafkaOutputBrokers))
        partition.foreach(record => {
          val data = record.toString
          val message = new ProducerRecord[String, String](kafkaOutputTopic, data)
          producer.send(message)

        })
        producer.close()
      })
    })
  }

  def setupKafkaProducer(kafkaOutputBrokers: String): util.HashMap[String, Object] = {
    val props = new util.HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaOutputBrokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props
  }

}
