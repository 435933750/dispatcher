package com.lx.util

import kafka.common.TopicAndPartition
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.kafka.OffsetRange

import scala.collection.JavaConverters._

class OffsetManager(getClient: () => ZkClient, getZkRoot: () => String) {

  // 定义为 lazy 实现了懒汉式的单例模式，解决了序列化问题，方便使用 broadcast
  lazy val zkClient: ZkClient = getClient()
  lazy val zkRoot: String = getZkRoot()

  // offsetId = md5(groupId+join(topics))
  // 初始化偏移量的 zk 存储路径 zkRoot
  def initOffset(): Unit = {
    if (!zkClient.exists(zkRoot)) {
      zkClient.createPersistent(zkRoot, true)
    }
  }

  // 从 zkRoot 读取偏移量信息
  def getOffset(): Map[TopicAndPartition, Long] = {
    val keys = zkClient.getChildren(zkRoot)
    var initOffsetMap: Map[TopicAndPartition, Long] = Map()
    if (!keys.isEmpty) {
      for (k: String <- keys.asScala) {
        val ks = k.split("!")
        val value: Long = zkClient.readData(zkRoot + "/" + k)
        initOffsetMap += (new TopicAndPartition(ks(0), Integer.parseInt(ks(1))) -> value)
      }
    }
    initOffsetMap
  }

  // 根据单条消息，更新偏移量信息
  def updateOffset(consumeRecord: ConsumerRecord[String, String]): Boolean = {
    val path = zkRoot + "/" + consumeRecord.topic + "!" + consumeRecord.partition
    zkClient.writeData(path, consumeRecord.offset())
    true
  }

  def createOffset(offsetRanges: Array[OffsetRange]) = {
    for (offset: OffsetRange <- offsetRanges) {
      val path = zkRoot + "/" + offset.topic + "!" + offset.partition
      if (!zkClient.exists(path)) {
        zkClient.createPersistent(path, offset.fromOffset)
      }
    }
  }

  // 消费消息后，批量提交偏移量信息
  def commitOffset(offsetRanges: Array[OffsetRange]): Boolean = {
    for (offset: OffsetRange <- offsetRanges) {
      val path = zkRoot + "/" + offset.topic + "!" + offset.partition
      if (!zkClient.exists(path)) {
        zkClient.createPersistent(path, offset.untilOffset)
      }
      else if (offset.untilOffset > offset.fromOffset) {
        zkClient.writeData(path, offset.untilOffset)
      }
    }
    true
  }

  override def finalize(): Unit = {
    zkClient.close()
  }
}

object OffsetManager {
  def apply(offsetId: String): OffsetManager = {
    val getClient = () => {
      new ZkClient(Constant.ZK_SERVERS, 30000)
    }
    val getZkRoot = () => {
      val zkRoot = Constant.ZK_ROOT + offsetId
      zkRoot
    }
    new OffsetManager(getClient, getZkRoot)
  }
}
