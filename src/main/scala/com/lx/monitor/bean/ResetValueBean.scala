package com.lx.monitor.bean

import com.lx.util.StringUtils

class ResetValueBean {
  var field: String = _
  var value: String = _
  var fromTable: String = _
  var hbaseTable: String = _
  var hbaseField: String = _
  var pk: String = _

  def needQueryHBase(): Boolean = {
    !StringUtils.isEmpty(hbaseTable, hbaseField)
  }

  override def toString: String = s"field=${field}\tvalue=${value}\tftable=${fromTable}\thtable=${hbaseTable}\thfield=${hbaseField}\tpk=${pk}"

}
