package com.lx.supplement.storage

import com.alibaba.fastjson.JSON
import com.lx.collecter.storage.ReceiveStorage
import com.lx.supplement.bean.{LineBean, PersistBean, ReceiveBean}
import com.lx.supplement.config.DataBaseConfig
import com.lx.supplement.dispatcher.SupplementDispatcher
import com.lx.util.HBaseUtil
import org.apache.commons.collections.CollectionUtils
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.apache.spark.broadcast.Broadcast
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

case class SupplementStorage(configBroad: Broadcast[Map[String, DataBaseConfig]], isBatch: Boolean = false) extends SupplementDispatcher(configBroad) {

  override val logger = LoggerFactory.getLogger(classOf[SupplementStorage])


  /**
    * 清理数据,筛选出配置过的表,并把配置回填
    *
    * @param value
    * @return (key:文件写出路径,value:文件内容)
    */
  override def clearData(kv: (Iterator[(String, String)], Map[String, DataBaseConfig])): Iterator[(String, LineBean)] = {
    val value = kv._1.toList
    val config = kv._2

    val lineBeans = ArrayBuffer[LineBean]()
    value.foreach(v => {

      try {
        val pb = JSON.parseObject(v._2, classOf[ReceiveBean])
        val dbConfig = config.get(pb.getDatabase)
        dbConfig match {
          case Some(x) => {
            val tableConfig = x.getTableMaps.get(pb.getTable)
            if (tableConfig != null) {
              //如果没有旧值，说明全是新插入，就不需要Zip
              if (CollectionUtils.isEmpty(pb.getOld)) {
                pb.getData().asScala.toList.foreach(map => {
                  val pk = map.get(tableConfig.getPk)
                  tableConfig.setDatabase(x.getDataBase)
                  lineBeans.append(LineBean(pk, pb.getDatabase, pb.getTable, pb.getTs, pb.getType, tableConfig, map.asScala.toMap, Map.empty[String, String]))
                })
              } else {
                val newMap = pb.getData().asScala.zip(pb.getOld.asScala).filter(_._1.containsKey(tableConfig.getPk))
                newMap.toList.foreach(doubleMap => {
                  val pk = doubleMap._1.get(tableConfig.getPk)
                  tableConfig.setDatabase(x.getDataBase)
                  lineBeans.append(LineBean(pk, pb.getDatabase, pb.getTable, pb.getTs, pb.getType, tableConfig, doubleMap._1.asScala.toMap, doubleMap._2.asScala.toMap))
                })
              }
            }
          }
          case None =>
        }
      } catch {
        case e: Exception => {
          logger.error(s"clearData error:${value}", e)
        }
      }
    })
    //todo 这里在batch是这样处理
    if (isBatch) {
      lineBeans.map(lb => (lb.pk, lb)).iterator
    } else {
      lineBeans.map(lb => (lb.dataBase + "-" + lb.table + "-" + lb.config.getOutput, lb)).iterator
    }
  }


  /**
    * 持久化数据
    * 一次调用是一个分区，一个分区是可能有多个key
    *
    * @param f
    * @param batchId
    */
  override def persist(f: Map[String, List[PersistBean]], batchId: String): Unit = {
    val conn = HBaseUtil.getConn
    if (conn == null)
      throw new Exception("persist get hbase conn is error")
    f.foreach(pb => {
      logger.error(s"hbase insert size:${pb._2.size}")
      val outTable = pb._1
      val table = conn.getTable(TableName.valueOf(outTable))
      val puts = pb._2.map(_.put)
      HBaseUtil.put(table, puts)
      table.close()
    })
  }


}
