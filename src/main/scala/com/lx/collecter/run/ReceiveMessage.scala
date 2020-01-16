package com.lx.collecter.run

import com.lx.collecter.storage.ReceiveStorage
import com.lx.util.Constant._
import com.lx.util.{Constant, Cypher, OffsetManager}
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory

object ReceiveMessage {

  //  val json = "1575902553395,appid/modelID,{\"modelId\":\"m01_serviceActive\",\"base\":{\"appId\":\"产品id\",\"userId\":\"服务端注册id\",\"status\":\"正常态|封禁态\",\"gender\":\"性别\",\"sexRate\":\"色情度\",\"country\":\"所在国别\",\"age\":\"年龄\",\"registdate\":\"注册时间\",\"role\":\"男|女|合作女|女神|上墙女神\",\"vip\":\"VIP等级A|B|C|FREE\",\"thumbs\":\"目前赞数，默认0\",\"coins\":\"目前金币数，默认0\",\"deviceId\":\"设备唯一key\",\"brand\":\"手机厂商\",\"model\":\"手机型号\",\"sysversion\":\"系统版本\",\"os\":\"android|IOS系统\",\"channel\":\"下载渠道\",\"version\":\"客户端版本\",\"date\":\"时间戳13位\"}}"

  val logger = LoggerFactory.getLogger(ReceiveMessage.getClass)
  //  val groupId = UUID.randomUUID().toString
  val groupId = Constant.GROUP_ID

  val kafkaParams = Map[String, String](
    BOOTSTRAP_SERVERS_CONFIG -> BOOTSTRAP_SERVERS,
    AUTO_OFFSET_RESET_CONFIG -> AUTO_OFFSET_RESET,
    GROUP_ID_CONFIG -> groupId,
    ENABLE_AUTO_COMMIT_CONFIG -> "false",
    SESSION_TIMEOUT_MS_CONFIG -> "20000",
    KEY_DESERIALIZER_CLASS_CONFIG -> classOf[org.apache.kafka.common.serialization.StringDeserializer].getName,
    VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[org.apache.kafka.common.serialization.StringDeserializer].getName
  )

  def main(args: Array[String]): Unit = {

    val privateKey = Cypher.getkeyFile(Constant.PRIVATEKEY_PATH)
    //        val sparkConf = new SparkConf().setAppName("ReceiveMessage").setMaster("local[5]")
    val sparkConf = new SparkConf().setAppName("ReceiveMessage")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "50000")
    sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true")
    sparkConf.set("spark.streaming.backpressure.enabled", "true")
    sparkConf.set("spark.streaming.backpressure.initialRate", "1000")
    sparkConf.set("spark.yarn.maxAppAttempts", "1")
    val ssc = new StreamingContext(sparkConf, Seconds(BATCH_SIZE.toInt))
    val kafkaOffset = OffsetManager(groupId)
    kafkaOffset.initOffset()
    val customOffset: Map[TopicAndPartition, Long] = kafkaOffset.getOffset()
    val pk = ssc.sparkContext.broadcast(privateKey)

    // 创建数据流
    val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.key, mmd.message)

    val stream = if (customOffset.isEmpty) {
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set(TOPICS.split(","): _*))
    } else {
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, customOffset, messageHandler)
    }

    val ctx = ReceiveContext(STOP_PATH, ssc)
    ReceiveStorage(TMP_SUFFIX, pk).handleData(stream, kafkaOffset, ctx)

    ctx.start()
  }


}
