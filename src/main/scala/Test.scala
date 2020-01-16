import java.util.Date

import com.lx.monitor.bean.ResetRowBean
import com.lx.util.{DateUtils, HBaseUtil}
import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Scan
import org.apache.kafka.clients.consumer.ConsumerConfig._
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.Nil
import scala.collection.mutable.ArrayBuffer

object Test {

  def main(args: Array[String]): Unit = {
  }

  def main2(args: Array[String]): Unit = {
    val kafkaParams = Map[String, String](
      BOOTSTRAP_SERVERS_CONFIG -> "b-8.athena-kafka-4p.oj1845.c3.kafka.eu-central-1.amazonaws.com:9092,b-4.athena-kafka-4p.oj1845.c3.kafka.eu-central-1.amazonaws.com:9092,b-6.athena-kafka-4p.oj1845.c3.kafka.eu-central-1.amazonaws.com:9092",
      AUTO_OFFSET_RESET_CONFIG -> "smallest",
      GROUP_ID_CONFIG -> "tttx",
      ENABLE_AUTO_COMMIT_CONFIG -> "false",
      SESSION_TIMEOUT_MS_CONFIG -> "20000",
      KEY_DESERIALIZER_CLASS_CONFIG -> classOf[org.apache.kafka.common.serialization.StringDeserializer].getName,
      VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[org.apache.kafka.common.serialization.StringDeserializer].getName
    )

    val sparkConf = new SparkConf().setAppName("Test")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

//    if (args.size > 1) {
//      println("set backpressure")
//      sparkConf.set("spark.streaming.backpressure.enabled", "true")
//      sparkConf.set("spark.streaming.backpressure.initialRate", args(1))
//    }
    if (args.size > 1) {
      println("set maxRatePerPartition")
      sparkConf.set("spark.streaming.kafka.maxRatePerPartition", args(1))
    }

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext, Seconds(args(0).toInt))
    ssc.sparkContext.setLogLevel("WARN")
    val ds = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set("canal_user"))
    val sdf = DateUtils.getSdf("yyyy-MM-dd HH:mm:ss")
    ds.foreachRDD(rdd => {
      val x = rdd.aggregate(0)((x,w)=>{x+1},(a,b)=>a+b)
      println(sdf.format(new Date()) + "==========" + rdd.getNumPartitions + "-" + x)
    })
    ssc.start()
    ssc.awaitTermination()

  }


  def getSsc(): StreamingContext = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("a")
    new StreamingContext(conf, Seconds(10))
  }

  def getSc(): SparkContext = {
    val conf = new SparkConf().setMaster("local").setAppName("a")
    new SparkContext(conf)
  }

  def getSs(): SparkSession = {
    val conf = new SparkConf().setMaster("local[3]").setAppName("a")
    SparkSession.builder().master("local").appName("a").getOrCreate()
  }
}
