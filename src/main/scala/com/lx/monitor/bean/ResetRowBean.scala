package com.lx.monitor.bean

import com.lx.util.StringUtils

case class ResetRowBean(pk: String, cols: Map[String, ResetValueBean]) {

  //检查当前行是否需要查询Hbase
  def needQueryHBase(): Boolean = {
    cols.filter(_._2.needQueryHBase()).size > 0
  }

  //查找当前行需要查找哪些habse
  def needQueryHBaseTables(): Set[String] = {
    cols.map(_._2.hbaseTable).filter(f=> !StringUtils.isEmpty(f)).toSet
  }

}
