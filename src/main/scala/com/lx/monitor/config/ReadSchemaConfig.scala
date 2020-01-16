package com.lx.monitor.config

import java.sql.Connection

import com.lx.monitor.bean.MonitorSchema
import com.lx.util.{MysqlUtil, StringUtils}

import scala.collection.mutable
import scala.tools.scalap.scalax.util.StringUtil

/**
  * 读取数据源配置
  */
object ReadSchemaConfig {

  @volatile private var schemaMap = mutable.Map[String, mutable.Map[String, MonitorSchema]]()

  def getConfig(): mutable.Map[String, mutable.Map[String, MonitorSchema]] = {
    if (schemaMap.isEmpty) {
      synchronized {
        if (schemaMap.isEmpty) {
          print(s"read config:${System.currentTimeMillis()}")
          read
        }
      }
    }
    schemaMap
  }

  private def read(): Unit = {
    var conn: Connection = null
    try {
      conn = MysqlUtil.getConn
      var result = conn.prepareStatement("SELECT b.display,a.read_hbase,a.source_table,a.read_key,a.fixed_value,a.`read_hbase_table`,a.`read_hbase_field`,a.`pk`,b.col_name FROM `monitor_schema_table` a INNER JOIN `monitor_schema_field` b ON a.`col_id`=b.id").executeQuery()
      while (result.next()) {
        val colName = result.getString("col_name")
        val sourceTable = result.getString("source_table")
        val readKey = result.getString("read_key")
        val fixedValue = result.getString("fixed_value")
        val readHbaseTable = result.getString("read_hbase_table")
        val readHbaseField = result.getString("read_hbase_field")
        val pk = result.getString("pk")
        val display = result.getInt("display")
        val readHbase = result.getBoolean("read_hbase")
        val map = schemaMap.getOrElseUpdate(sourceTable, mutable.Map[String, MonitorSchema]())
        map.put(/*if (StringUtils.isEmpty(readKey)) colName else readKey*/ colName, MonitorSchema(sourceTable, colName, readKey, fixedValue, readHbaseTable, readHbaseField, pk, display, readHbase))
      }
    }
    finally {
      if (conn != null && (!conn.isClosed)) conn.close()
    }
  }
}
