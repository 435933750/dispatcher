package com.lx.monitor.run

import com.lx.RuntimeContext
import com.lx.monitor.storage.MonitorStorage
import com.lx.monitor.task.ScheduleTask
import com.lx.util.{Constant, DateUtils}
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory

object StartMonitor {

  val sdf = DateUtils.getSdf("yyyy-MM-dd HH:mm:ss")
  val sdf_h = DateUtils.getSdf("yyyyMMddHH")
  val logger = LoggerFactory.getLogger(StartMonitor.getClass)

  def main(args: Array[String]): Unit = {

    val batchSize = 60
    val topics = "canal_user,canal_stat,canal_pay,canal_raffle"
    //        val topics = "ssc"
    val groupId = "gid"
    val stopPath = "/ssc/c3/stop"


    val kafkaParams = Map[String, String](
      BOOTSTRAP_SERVERS_CONFIG -> Constant.BOOTSTRAP_SERVERS,
      AUTO_OFFSET_RESET_CONFIG -> Constant.AUTO_OFFSET_RESET,
      GROUP_ID_CONFIG -> groupId,
      ENABLE_AUTO_COMMIT_CONFIG -> "false",
      SESSION_TIMEOUT_MS_CONFIG -> "20000",
      KEY_DESERIALIZER_CLASS_CONFIG -> classOf[org.apache.kafka.common.serialization.StringDeserializer].getName,
      VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[org.apache.kafka.common.serialization.StringDeserializer].getName
    )

    val sparkConf = new SparkConf().setAppName("Monitor") /*.setMaster("local[2]")*/
//    sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "10000")
    //    sparkConf.set("spark.streaming.backpressure.enabled", "true")
    //    sparkConf.set("spark.streaming.backpressure.initialRate", "1000")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true")
    sparkConf.set("spark.yarn.maxAppAttempts", "1")
    sparkConf.set("hive.exec.dynamici.partition", "true")
    sparkConf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    sparkConf.set("spark.sql.warehouse.dir", "/user/hive/warehouse")

    val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext, Seconds(batchSize))
    ssc.sparkContext.setLogLevel("WARN")
    val ds = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set(topics.split(","): _*))
    val ctx = RuntimeContext(stopPath, ssc)
    MonitorStorage(spark).handleData(ds, null, ctx)
    ScheduleTask.startScan(spark)
    ctx.start()
  }


}
