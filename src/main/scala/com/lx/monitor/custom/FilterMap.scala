package com.lx.monitor.custom

import com.lx.supplement.bean.ReceiveBean

/**
  * 过滤
  */
object FilterMap {


  def filter(rb: ReceiveBean) = {
    var rt = true
    if (rb.getType != "INSERT") {
      rt = false
    }
    rt
  }
}
