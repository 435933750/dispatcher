package com.lx.collecter.dispatcher

import java.util.UUID

import com.lx.{Dispatcher, RuntimeContext}
import com.lx.util.{Constant, FsUtils, OffsetManager}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.HasOffsetRanges
import org.slf4j.LoggerFactory

abstract class ReceiveDispatcher extends Dispatcher[String, String, Iterator[(String, String)], Iterator[(String, String)], Iterator[(String, Iterable[String])]] {


  val logger = LoggerFactory.getLogger(classOf[ReceiveDispatcher])

  /**
    * 处理数据入口
    *
    * @param ds
    */
  override def handleData(ds: InputDStream[(String, String)], kakfaOfs: OffsetManager, ctx: RuntimeContext): Unit = {

    ds.foreachRDD(rdd => {

      val batchId = UUID.randomUUID().toString.replaceAll("-", "")
      logger.warn(s"batchId-${batchId}开始")

      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      offsetRanges.foreach(o => {
        logger.warn(s"ofs=>${o.topic}-${o.partition}-${o.fromOffset}-${o.untilOffset}")
      })
      kakfaOfs.createOffset(offsetRanges)

      var stop = false
      try {
        rdd.mapPartitions(clearData).filter(x => (!x._1.isEmpty)).groupByKey(10)
          .foreachPartition(x => persist(x, batchId))
      } catch {
        case e: Exception =>
          logger.error("system err,stoping service...", e);
          stop = true
      }


      stop match {
        case true => ctx.stop //这里使用非优雅关闭,避免下次rdd继续进来
        case false => {
          val fs = FsUtils()
          try {
            fs.complate(Constant.UNMERGE_PATH, x => x.contains(batchId))
            kakfaOfs.commitOffset(offsetRanges)
          } catch {
            case e: Exception => {
              logger.error("complate file with hdfs or commit offset err,stoping service...", e)
              ctx.stop
            }
          }
        }
      }
      logger.warn(s"batchId=${batchId}结束")
    })

  }

}

