package kafka

import java.util
import java.util.{Properties, UUID}

import com.lx.util.Cypher
import org.apache.kafka.clients.consumer.KafkaConsumer

import scala.collection.JavaConverters._

object TestKafkaConsumer {

  val privateKeyPath = "G:\\莱熙\\宿荣全\\private.der"
  val pk = Cypher.getkeyFile(privateKeyPath)


  def main(args: Array[String]): Unit = {
    //    canal_user,canal_stat,canal_pay,canal_raffle
    val topic = "canal_user"
    //    val bs = "master:9092,slave1:9092,slave2:9092"
    //    val bs = "10.0.3.103:9092,10.0.3.90:9092,10.0.3.92:9092"

    val bs = "b-8.athena-kafka-4p.oj1845.c3.kafka.eu-central-1.amazonaws.com:9092,b-4.athena-kafka-4p.oj1845.c3.kafka.eu-central-1.amazonaws.com:9092,b-6.athena-kafka-4p.oj1845.c3.kafka.eu-central-1.amazonaws.com:9092"
    val groupId = UUID.randomUUID().toString
    val props = new Properties
    props.put("bootstrap.servers", bs)
    props.put("group.id", groupId)
    props.put("enable.auto.commit", "true")
    props.put("session.timeout.ms", "30000")
    props.put("auto.offset.reset", "latest")
    props.put("key.deserializer", classOf[org.apache.kafka.common.serialization.StringDeserializer].getName)
    props.put("value.deserializer", classOf[org.apache.kafka.common.serialization.StringDeserializer].getName)

    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(util.Arrays.asList(topic))
    val partitions = consumer.partitionsFor(topic).asScala
    partitions.foreach(p => println("host=>" + p.leader().host() + "->" + p.topic()))
//rc_report_record 封号
    while (true) {
      val msg = consumer.poll(1000).asScala
      msg.foreach(record => {
//        if (record.value().contains("rc_user_pay_record")&&record.value().contains("update")) {
          println(record.value())
//        }
//        if(list.filter(record.value().contains).size>0){
//          println(record.value())
//        }
      })
    }
  }


}
