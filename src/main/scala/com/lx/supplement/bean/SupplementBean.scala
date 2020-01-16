package com.lx.supplement.bean

import com.lx.supplement.config.{ActionEnum, HandleEnum, TableConfig}
import com.lx.supplement.custom.CustomConfig
import com.lx.util.{Constant, MapUtils}
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.util.Bytes
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable

class SupplementBean(lbs: List[LineBean], hbaseResult: Array[Result], realTime: Boolean = true) {
  val logger = LoggerFactory.getLogger(classOf[SupplementBean])
  val cfBytes = Constant.SUPPLEMENT_FM

  def executor(): List[PersistBean] = {
    val conf: TableConfig = lbs.head.config
    conf.getHandle.asScala.map(handle => {
      val puts = handle._1 match {
        case HandleEnum.UPDATE => handleUpdate(handle._2.asScala)
        case HandleEnum.SUM => if (realTime) handleSum(handle._2.asScala) else Nil
        case HandleEnum.CUSTOM => handleCustom(handle._2.asScala)
        case _ => handleOther(handle._2.asScala)
      }
      puts.filter(!_.isEmpty) map (p => PersistBean(conf.getOutput, p))
    }).flatten.toList
  }

  def handleUpdate(configMap: mutable.Map[String, String]): List[Put] = {
    lbs.map(lb => {
      //筛选出配置了的属性数据
      val data = filterFieldFromConfig(lb, configMap)
      if (!data.isEmpty) {
        val put = new Put(Bytes.toBytes(lb.pk), lb.ts.getTime)
        data.map(col => {
          put.addColumn(cfBytes, Bytes.toBytes(configMap.get(col._1).get), Bytes.toBytes(col._2))
        })
        put
      } else {
        null
      }
    }).filter(_ != null)

  }


  def handleSum(configMap: mutable.Map[String, String]): List[Put] = {
    //先按rowkey分组
    val groupByRowkey = lbs.groupBy(_.pk)

    val aggred = groupByRowkey.map(g => {
      //聚合当前rowkey的所有sum值
      var lb: LineBean = null
      try {
        lb = g._2.map(lb => {
          val nlb = lb.replica()
          nlb.data = filterFieldFromConfig(lb, configMap)
          nlb
        }).reduceLeft((rb1, rb2) => {
          val left = rb1.data.mapValues(_.toDouble)
          val right = rb2.data.mapValues(_.toDouble)
          val reduced = (left /: right) ((map, kv) => {
            map + (kv._1 -> (kv._2 + map.getOrElse(kv._1, 0.0)))
          })
          rb2.data = reduced.mapValues(_.toString)
          rb2
        })
      } catch {
        case e: Exception => logger.error(s"aggregation sum error:rowkey=${g._1}", e);
      }
      lb
    }).filter(_ != null)

    val resultMap = hbaseResult.map(r => if (r.isEmpty) (null, null) else (new String(r.getRow), r)).filter(_._1 != null).toMap

    if (!aggred.isEmpty) {
      aggred.map(rb => {
        val rowkey = rb.pk
        val data = rb.data
        var put: Put = null
        if (!MapUtils.isEmpty(data)) {
          put = new Put(rowkey.getBytes(), System.currentTimeMillis())
          val result = resultMap.getOrElse(rowkey, null)
          data.foreach(kv => {
            val remoteField = configMap.get(kv._1).get
            val oldValue = try {
              if (result != null) Bytes.toString(result.getColumnLatestCell(cfBytes, Bytes.toBytes(remoteField)).getValue).toDouble
              else 0.0
            } catch {
              case e: Exception => e.printStackTrace(); 0.0
            }
            val newValue = try {
              kv._2.toDouble
            } catch {
              case e: Exception => e.printStackTrace(); 0.0
            }
            put.addColumn(cfBytes, Bytes.toBytes(remoteField), Bytes.toBytes((newValue + oldValue).toString))
          })
        }
        put
      }).filter(_ != null).toList
    } else {
      Nil
    }
  }


  def handleCustom(configMap: mutable.Map[String, String]): List[Put] = {
    val customConfigMap = configMap.filter(_._1.startsWith("class"))
    //自定义解析
    if (!customConfigMap.isEmpty) CustomConfig.custom(lbs, hbaseResult, customConfigMap)
    else Nil
  }

  /**
    * 从数据源怕匹配出来配置里面监听的属性
    *
    * @param rb
    * @param configMap
    * @return
    */
  def filterFieldFromConfig(lb: LineBean, configMap: mutable.Map[String, String]) = {
    val dataMap = lb.data
    ActionEnum.get(lb.ttype) match {
      case ActionEnum.UPDATE => {
        if (!MapUtils.isEmpty(lb.old)) {
          dataMap.filter(kv => (configMap.get(kv._1) != None) && lb.old.contains(kv._1))
        } else Map.empty[String, String]
      }
      case ActionEnum.INSERT => {
        dataMap.filter(kv => (configMap.get(kv._1) != None))
      }
      case _ => Map.empty[String, String]
    }
  }

  def handleOther(map: mutable.Map[String, String]): List[Put] = {
    Nil
  }
}
