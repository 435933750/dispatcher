package kafka

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId}
import java.util.concurrent.CountDownLatch
import java.util.{Date, Properties}

import com.lx.util.{ByteUtil, Cypher}
import org.apache.commons.httpclient.HttpClient
import org.apache.commons.httpclient.methods.GetMethod
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer

import scala.util.Random

object TestKafkaProducer {


  def main(args: Array[String]): Unit = {
    sendKafka()
  }


  def sendKafka(): Unit = {
    val topic = "yw_test"
//    val bs = "192.168.1.102:9092,192.168.1.103:9092,192.168.1.104:9092"
        val bs = "10.0.3.103:9092,10.0.3.90:9092,10.0.3.92:9092"
    val props = new Properties
    props.put("bootstrap.servers", bs)
    props.put("retries", "0")
    props.put("batch.size", "1")

    props.put("key.serializer", classOf[StringSerializer].getName)
    props.put("value.serializer", classOf[StringSerializer].getName)
    val producer = new KafkaProducer[String, String](props)
    val num = 100
    val c = new CountDownLatch(num)
    val start = System.currentTimeMillis()
    for (i <- 1 to num) {
            val time = timestamp2String(System.currentTimeMillis() - 28800000)
//      val time = timestamp2String(System.currentTimeMillis())
//      val appid = Random.nextInt(3)
      val appid =0
      val modelID = Random.nextInt(3)
      val str = s"${time},${appid}/${modelID},{${appid},${modelID},a}"
      producer.send(new ProducerRecord[String, String](topic, null, ByteUtil.bytesToHexString(encryptMessage(str))), new Callback {
        override def onCompletion(recordMetadata: RecordMetadata, e: Exception): Unit = {
          println(c.getCount)
          c.countDown()
        }
      })
      //      producer.send(new ProducerRecord[String, String](topic, null, str)).get()
    }
    c.await()
    println(s"生产1000万用时:${(System.currentTimeMillis() - start) / 1000}秒")
  }


  val publicKeyPath = "G:\\莱熙\\宿荣全\\public.der"
  val publickey = Cypher.getkeyFile(publicKeyPath)

  def encryptMessage(message: String): Array[Byte] = {
    val random = Cypher.randomNum(8)
    var rsa = Cypher.encrypt_rsa(random.getBytes(), publickey)
    var des = Cypher.encrypt_des(message.getBytes(), random.getBytes)
    rsa ++ des
  }

  def timestamp2String(time: Long) = {
    val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")
    val instant = new Date(time).toInstant
    formatter.format(LocalDateTime.ofInstant(instant, ZoneId.systemDefault()))
  }

  def sendHttp(): Unit = {
    val client = new HttpClient
    //把中文转成Unicode编码
    // 使用 GET 方法 ，如果服务器需要通过 HTTPS 连接，那只需要将下面 URL 中的 http 换成 https
    val method = new GetMethod
    val start = System.currentTimeMillis()
    for (i <- 1 to 10000) {
      val time = System.currentTimeMillis()
      val appid = Random.nextInt(3)
      val modelID = Random.nextInt(12)
      val str = s"${appid}/${modelID},12312312321313213213"
      val method = new GetMethod("http://master:8100/ssc?" + ByteUtil.bytesToHexString(encryptMessage(str)))
      client.executeMethod(method)
      println(i)
    }
    method.releaseConnection()
    println(s"生产1000万用时:${(System.currentTimeMillis() - start) / 1000}秒")
  }

}
