package com.lx.supplement.dispatcher

import java.util.UUID

import com.lx.supplement.bean.{LineBean, PersistBean, SupplementBean}
import com.lx.supplement.config.DataBaseConfig
import com.lx.util.{FsUtils, HBaseUtil, OffsetManager}
import com.lx.{Dispatcher, RuntimeContext}
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Get, Result}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.HasOffsetRanges
import org.slf4j.LoggerFactory

import scala.collection.immutable.Nil

abstract class SupplementDispatcher(configBroad: Broadcast[Map[String, DataBaseConfig]]) extends Dispatcher[String, String, (Iterator[(String, String)], Map[String, DataBaseConfig]), Iterator[(String, LineBean)], Map[String, List[PersistBean]]] {


  val logger = LoggerFactory.getLogger(classOf[SupplementDispatcher])

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
        //把所有数据用 database+table+output组成key的分组,同一个表下的数据肯定在一个分区
        val group = rdd.mapPartitions(line => clearData(line, configBroad.value)).groupByKey(10)

        group.foreachPartition(p => {
          val list = p.toList
          if (!list.isEmpty) {
            val splits = list.head._1.split("-")
            val sourceDB = splits(0)
            val sourceTB = splits(1)
            val targetTB = splits(2)
            //先查询该表所有pk在Hbase的值

            val pkSet = list.flatMap(_._2.toList).map(_.pk).toSet

            val hbaseResult: Array[Result] = if (!pkSet.isEmpty) {
              val conn = HBaseUtil.getConn
              if (conn == null)
                throw new Exception("hbase connection is null")
              val outputTable = conn.getTable(TableName.valueOf(targetTB))
              val results = HBaseUtil.get(outputTable, pkSet.map(pk => new Get(Bytes.toBytes(pk))).toList: _*)
              outputTable.close()
              results
            } else {
              Nil.asInstanceOf[Array[Result]]
            }
            val groupByOutoutTable = list.map(kv => {
              new SupplementBean(kv._2.toList, hbaseResult).executor()
            }).flatten.groupBy(_.tableName)
            persist(groupByOutoutTable, batchId)
          }
        })

      } catch {
        case e: Exception =>
          logger.error("system err,stoping service...", e);
          stop = true
      }


      stop match {
        case true => ctx.stop //这里使用非优雅关闭,避免下次rdd继续进来
        case false => {
          try {
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

