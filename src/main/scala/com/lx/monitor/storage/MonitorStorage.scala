package com.lx.monitor.storage

import com.alibaba.fastjson.JSON
import com.lx.monitor.bean.{MonitorSchema, ResetRowBean, ResetValueBean}
import com.lx.monitor.custom.{CustomMap, FilterMap}
import com.lx.monitor.dispatcher.MonitorDispatcher
import com.lx.supplement.bean.{PersistBean, ReceiveBean}
import com.lx.util.{Constant, DateUtils, HBaseUtil, StringUtils}
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Get, Result}
import org.apache.hadoop.hbase.util.{Bytes, CollectionUtils}
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.JavaConverters._
import scala.collection.immutable.Nil
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

case class MonitorStorage(spark: SparkSession) extends MonitorDispatcher(spark) {

  val cf = Bytes.toBytes("c1")

  val sdf = DateUtils.getSdf("yyyy-MM-dd HH:mm:ss")
  val sdf_h = DateUtils.getSdf("yyyy-MM-dd HH")

  /**
    * 清理转换数据
    *
    * @param value
    * @return
    */
  override def clearData(value: (Iterator[(String, String)], mutable.Map[String, mutable.Map[String, MonitorSchema]], List[String])): Iterator[Row] = {
    try {
      val totalData = new ArrayBuffer[ResetRowBean]()
      val data = value._1.toList.map(_._2)
      val schemaMap = value._2
      val titles = value._3
      if (data == null || data.isEmpty) {
        return Nil.iterator
      }


      //先根据是否读取hbase，是否有固定值等等封装对象
      val resets = data.flatMap(m => {
        try {
          val rb = JSON.parseObject(m, classOf[ReceiveBean])
          //如果是rc_user,则只关心INSERT
          if (!FilterMap.filter(rb)) {
            Nil
          } else {
            resetValue(rb, schemaMap)
          }

        } catch {
          case e: Exception => {
            logger.error(s"monitor clear map Error", e);
            Nil
          }
        }
      })




      var needQueryHbase = resets.filter(_.needQueryHBase())
      var dontNeedQueryHbase = resets.filter(!_.needQueryHBase())
      totalData ++= dontNeedQueryHbase


      //把需要读取Hbase的部分取出来一次性读取
      if (!needQueryHbase.isEmpty) {
        val conn = HBaseUtil.getConn
        if (conn == null)
          throw new Exception("hbase connection is null")

        //先查找出所有行需要查询的所有表名
        val needQueryTableSet = needQueryHbase.flatMap(_.needQueryHBaseTables()).toSet
        //循环每个表,建立HTable对象
        needQueryTableSet.foreach(tableName => {
          val table = conn.getTable(TableName.valueOf(tableName))

          //筛选出每一个需要查询此表的列
          val cols = needQueryHbase.flatMap(row => {
            row.cols.filter(_._2.hbaseTable == tableName)
          })
          if (!cols.isEmpty) {
            //拿到所有需要检索的rowkey并且查询
            val rowkeys = cols.map(m => new Get(Bytes.toBytes(m._2.pk))).distinct.asJava
            val results = table.get(rowkeys).map(m => (Bytes.toString(m.getRow), m)).filter(!_._2.isEmpty).toMap
            table.close()

            cols.foreach(col => {
              results.get(col._2.pk) match {
                case Some(x) => {
                  val cell = x.getColumnLatestCell(cf, Bytes.toBytes(col._2.hbaseField))
                  col._2.value = if (cell != null && cell.getValue != null) {
                    Bytes.toString(cell.getValue)
                  } else {
                    Constant.MONITOR_UNKNOW_VALUE
                  }
                }
                case None => col._2.value = Constant.MONITOR_UNKNOW_VALUE
              }
            })
          }
        })
      }
      totalData ++= needQueryHbase

      //自定义过滤
      totalData.foreach(row => {
        row.cols.foreach(col => {
          CustomMap.get(col._2.fromTable, col._2.field) match {
            case Some(func) => {
              col._2.value = func(row.cols)
            }
            case None =>
          }
        })
      })



      //Bean转换成HBase Row
      totalData.map(data => {
        val columns = ArrayBuffer[String]()
        val fieldMap = data.cols.groupBy(_._2.field).map(m => (m._1, m._2.head))
        titles.foreach(t => {
          fieldMap.get(t) match {
            case Some(x) => columns += x._2.value
            case None => columns += Constant.MONITOR_NOSET_VALUE
          }
        })
        Row.fromSeq(columns.seq)
      }).iterator
    } catch {
      case e: Exception => logger.error("monitor clearData error", e); Nil.iterator
    }

  }

  /**
    * 根据配置文件重置值
    *
    * @param rb
    * @param schemaMap
    */
  def resetValue(rb: ReceiveBean, schemaMap: mutable.Map[String, mutable.Map[String, MonitorSchema]]): ArrayBuffer[ResetRowBean] = {
    val rows = ArrayBuffer[ResetRowBean]()

    schemaMap.get(rb.getTable) match {
      case Some(s) => {
        if (!CollectionUtils.isEmpty(rb.getData)) {
          rb.getData.asScala.foreach(map => {
            val colMap = map.asScala
            //把ts填充到每一个map
            colMap.put("ts", sdf.format(rb.getTs.getTime))
            var resetList = ArrayBuffer[ResetValueBean]()
            var pk = ""
            s.foreach(configKV => {
              val schemaCol = configKV._1
              val schemaAppointKey = configKV._2.readKey
              val schema = configKV._2
              val reset = new ResetValueBean()
              //只有不是对端列的时候，才设置本行数据的主键
              pk = if (StringUtils.isEmpty(pk) && !schema.colName.startsWith("remote_")) colMap.getOrElse(schema.pk, "") else pk
              reset.pk = colMap.getOrElse(schema.pk, "")

              colMap.get(if (StringUtils.isEmpty(schemaAppointKey)) schemaCol else schemaAppointKey) match {
                case Some(value) => {
                  reset.fromTable = schema.sourceTable
                  reset.value = value
                  reset.field = schema.colName
                  if (!StringUtils.isEmpty(schema.fixedValue)) {
                    reset.value = schema.fixedValue
                  } else if (!StringUtils.isEmpty(schema.readHbaseTable) && (!StringUtils.isEmpty(schema.readHbaseField))) {
                    reset.hbaseTable = schema.readHbaseTable
                    reset.hbaseField = schema.readHbaseField
                  }
                }
                case None => {
                  reset.fromTable = schema.sourceTable
                  reset.field = schema.colName
                  if (!StringUtils.isEmpty(schema.fixedValue)) {
                    reset.value = schema.fixedValue
                  } else {
                    reset.hbaseTable = schema.readHbaseTable
                    reset.hbaseField = schema.readHbaseField
                  }
                }
              }
              resetList.append(reset)
            })

            if (!StringUtils.isEmpty(pk)) {
              rows.append(ResetRowBean(pk, resetList.filter(!_.pk.isEmpty).map(m => (m.field, m)).toMap))
            }

          })
        }
      }
      case None =>
    }
    rows
  }

  /**
    * 持久化数据
    *
    * @param f
    */
  override def persist(f: Map[String, List[PersistBean]], batchId: String): Unit = {

  }
}
