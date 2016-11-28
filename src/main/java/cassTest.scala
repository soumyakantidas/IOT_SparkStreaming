import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by soumyaka on 11/28/2016.
  */
object cassTest {
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

    val someRDD = ssc.cassandraTable("iot", "user_details").select("user_id", "category")
      .joinWithCassandraTable("iot", "userhistory", selectedColumns = SomeColumns("user_id",
        "date"), joinColumns = SomeColumns("user_id"))

    someRDD.foreach(println)

  }

}
