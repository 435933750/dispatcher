package com.lx.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer


object HBaseUtil {

  val logger = LoggerFactory.getLogger(HBaseUtil.getClass)
  private var conf: Configuration = _
  @volatile private var conn: Connection = _
  init

  private def init(): Unit = {
    conf = HBaseConfiguration.create()
    //      conf.set("hbase.zookeeper.quorum", "master,slave1,slave2")
    conf.set("hbase.zookeeper.quorum", "ip-10-0-6-232.eu-central-1.compute.internal,ip-10-0-6-201.eu-central-1.compute.internal,ip-10-0-6-123.eu-central-1.compute.internal")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("yarn.resourcemanager.hostname", "ip-10-0-6-201.eu-central-1.compute.internal")
    conf.set("hbase.rpc.timeout", "5000")
  }

  def getConn: Connection = {
    try {
      if (conn == null || conn.isClosed()) {
        synchronized {
          if (conn == null || conn.isClosed()) {
            conn = ConnectionFactory.createConnection(conf)
          }
        }
      }
    } catch {
      case e: Exception => {
        logger.error("HBaseInit error", e)
      }
    }
    conn
  }


  def close(): Unit = {
    if (conn != null && (!conn.isClosed)) conn.close()
  }

  def scan = {
    val tb = conn.getTable(TableName.valueOf("t1"))
    println(tb.getScanner(new Scan()).next())
  }


  def put(table: Table, row: String, cf: String, col: String, data: Array[Byte]): Unit = {
    val p = new Put(row.getBytes())
    p.addColumn(cf.getBytes(), col.getBytes(), data)
    put(table, p)
  }

  def put(table: Table, put: Put): Unit = {
    table.put(put)
  }

  def put(table: Table, puts: List[Put]): Unit = {
    val save = new ArrayBuffer[Put](10001)
    for (i <- 0 until puts.size) {
      save.append(puts(i))
      if (i % 10000 == 0) {
        table.put(save.toList.asJava)
        save.clear()
      }
    }
    table.put(save.toList.asJava)
  }

  def get(table: Table, get: Get*): Array[Result] = {
    var result = new ArrayBuffer[Result](get.size)
    val gets = ArrayBuffer[Get]()
    for (i <- 0 until get.size) {
      gets.append(get(i))
      if (i % 10000 == 0) {
        result.append(table.get(gets.toList.asJava): _*)
        gets.clear()
      }
    }
    result.append(table.get(gets.toList.asJava): _*)
    result.toArray
  }

  def getPut(rowKey: String): Put = {
    new Put(Bytes.toBytes(rowKey))
  }

  def getIncr(rowKey: String): Increment = {
    new Increment(Bytes.toBytes(rowKey))
  }

}


//case class HBaseUtil(initTable: String = null) {
//
//  val logger = LoggerFactory.getLogger(classOf[HBaseUtil])
//  private var conf: Configuration = _
//  private var conn: Connection = _
//  private var table: Table = _
//  init
//
//  private def init(): Unit = {
//    try {
//      conf = HBaseConfiguration.create()
//      conf.set("hbase.zookeeper.quorum", "master,slave1,slave2")
//      conf.set("hbase.zookeeper.property.clientPort", "2181")
//      conf.set("hbase.rpc.timeout", "5000")
//      conn = ConnectionFactory.createConnection(conf)
//      if (!StringUtils.isEmpty(initTable))
//        table = conn.getTable(TableName.valueOf(initTable))
//    } catch {
//      case e: Exception => {
//        println("HBase init error", e)
//      }
//    }
//  }
//
//  def setTable(tableName: String): Unit = {
//    closeTable(tableName)
//    table = conn.getTable(TableName.valueOf(tableName))
//  }
//  def closeTable(tableName: String): Unit = {
//    if (table != null) {
//      table.close()
//    }
//  }
//
//  def close(): Unit = {
//    if (table != null) table.close()
//    if (conn != null && (!conn.isClosed)) conn.close()
//  }
//
//  def scan = {
//    val tb = conn.getTable(TableName.valueOf("t1"))
//    println(tb.getScanner(new Scan()).next())
//  }
//
//
//
//  def put(row: String, cf: String, col: String, data: Array[Byte]): Unit = {
//    val p = new Put(row.getBytes())
//    p.addColumn(cf.getBytes(), col.getBytes(), data)
//    put(p)
//  }
//
//  def put(put: Put): Unit = {
//    table.put(put)
//  }
//
//  def put(puts: List[Put]): Unit = {
//    table.put(puts.asJava)
//  }
//
//  def get(get: Get*): Array[Result] = {
//    table.get(get.toList.asJava)
//  }
//
//
//}
