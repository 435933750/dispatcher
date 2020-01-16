package com.lx.supplement.bean

import java.sql.Timestamp

import com.lx.supplement.config.TableConfig
import org.apache.commons.beanutils.BeanUtils

class LineBean extends Serializable {
  var pk: String = _
  var dataBase: String = _
  var table: String = _
  var ts: Timestamp = _
  var ttype: String = _
  var config: TableConfig = _
  var data: Map[String, String] = _
  var old: Map[String, String] = _

  def replica(): LineBean = {
    val newLb = LineBean()
    val newConfig = new TableConfig()
    BeanUtils.copyProperties(newConfig, config)
    var newData = Map[String, String]()
    data.foreach(kv => newData += kv._1 -> kv._2)
    var newOld = Map[String, String]()
    old.foreach(kv => newOld += kv._1 -> kv._2)
    newLb.pk = pk
    newLb.dataBase = dataBase
    newLb.table = table
    newLb.ts = ts
    newLb.ttype = ttype
    newLb.config = newConfig
    newLb.data = newData
    newLb.old = newOld
    newLb
  }

}

object LineBean {
  def apply(pk: String, dataBase: String, table: String, ts: Timestamp, ttype: String, config: TableConfig, data: Map[String, String], old: Map[String, String]): LineBean = {
    val lb = new LineBean
    lb.pk = pk
    lb.dataBase = dataBase
    lb.table = table
    lb.ts = ts
    lb.ttype = ttype
    lb.config = config
    lb.data = data
    lb.old = old
    lb
  }


  def apply() = new LineBean
}

