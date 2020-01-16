package com.lx.monitor.bean

/**
  *
  * @param sourceTable      监听表
  * @param colName          输出字段(监听字段)
  * @param readKey          如果输出字段和监听字段不同，则在这里设置监听字段
  * @param fixedValue       固定值
  * @param readHbaseTable   读取hbase表
  * @param read_hbase_field 读取hbase字段
  * @param display 是否展示 1=展示，2=不展示
  */
case class MonitorSchema(sourceTable: String, colName: String, readKey: String, fixedValue: String, readHbaseTable: String, readHbaseField: String, pk: String, display: Int,readHbase:Boolean)
