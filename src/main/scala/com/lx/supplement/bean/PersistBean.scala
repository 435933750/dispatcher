package com.lx.supplement.bean

import org.apache.hadoop.hbase.client.Put

case class PersistBean(tableName: String, put: Put) extends Serializable
