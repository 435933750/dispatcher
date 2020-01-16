package com.lx.supplement.run

import com.lx.RuntimeContext
import com.lx.supplement.config.DataBaseConfig
import com.lx.supplement.storage.SupplementStorage
import com.lx.util.Constant._
import com.lx.util.{Constant, OffsetManager}
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.commons.beanutils.BeanUtils
import org.apache.kafka.clients.consumer.ConsumerConfig._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

object StartSupple {


  val logger = LoggerFactory.getLogger(StartSupple.getClass)
  val groupId = Constant.SUPPLEMENT_GROUP_ID
  //  val groupId = UUID.randomUUID().toString

  val kafkaParams = Map[String, String](
    BOOTSTRAP_SERVERS_CONFIG -> SUPPLEMENT_BOOTSTRAP_SERVERS,
    AUTO_OFFSET_RESET_CONFIG -> SUPPLEMENT_AUTO_OFFSET_RESET,
    GROUP_ID_CONFIG -> groupId,
    ENABLE_AUTO_COMMIT_CONFIG -> "false",
    SESSION_TIMEOUT_MS_CONFIG -> "20000",
    KEY_DESERIALIZER_CLASS_CONFIG -> classOf[org.apache.kafka.common.serialization.StringDeserializer].getName,
    VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[org.apache.kafka.common.serialization.StringDeserializer].getName
  )

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("Supple")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "1000")
    sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true")
//    sparkConf.set("spark.streaming.backpressure.enabled", "true")
//    sparkConf.set("spark.streaming.backpressure.initialRate", "5000")
//    sparkConf.set("spark.streaming.backpressure.pid.minRate", "8000")
    sparkConf.set("spark.yarn.maxAppAttempts", "1")
    sparkConf.set("spark.storage.memoryFraction", "0.3")
    //    --conf spark.driver.extraJavaOptions="-XX:PermSize=1024M -XX:MaxPermSize=2048M"
    val ssc = new StreamingContext(sparkConf, Seconds(SUPPLEMENT_BATCH_SIZE.toInt))
    ssc.sparkContext.setLogLevel("WARN")
    val kafkaOffset = OffsetManager(groupId)
    kafkaOffset.initOffset()
    val customOffset: Map[TopicAndPartition, Long] = kafkaOffset.getOffset()
    // 创建数据流
    val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.key, mmd.message)

    val stream = if (customOffset.isEmpty) {
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set(SUPPLEMENT_TOPICS.split(","): _*))
    } else {
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, customOffset, messageHandler)
    }

    val ctx = RuntimeContext(SUPPLEMENT_STOP_PATH, ssc)
    val config = DataBaseConfig.createDatBaseConf()
    val configBroad = ctx.ssc.sparkContext.broadcast(config.asScala.toMap)
    SupplementStorage(configBroad).handleData(stream, kafkaOffset, ctx)

    ctx.start()
  }

}
