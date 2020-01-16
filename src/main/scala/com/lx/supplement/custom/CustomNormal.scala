package com.lx.supplement.custom

import com.lx.supplement.bean.LineBean
import com.lx.util.Constant
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.util.Bytes

/**
  * 普通人身份
  */
class CustomNormal extends Custom {
  override def custom(lbs: List[LineBean], rus: Array[Result], config: (String, String)): List[Put] = {
    val ruMap = rus.map(r => (Bytes.toString(r.getRow), r)).toMap
    lbs.filter(lb => {
      var flag = true
      ruMap.get(lb.pk) match {
        case Some(x) => {
          val cell = x.getColumnLatestCell(Constant.SUPPLEMENT_FM, Bytes.toBytes(config._2))
          if (cell != null && !Bytes.toString(cell.getValue).isEmpty) {
            flag = false
          }
        }
        case None =>
      }
      flag
    }).map(lb => {
      val put = new Put(Bytes.toBytes(lb.pk))
      put.addColumn(Constant.SUPPLEMENT_FM, Bytes.toBytes(config._2), lb.ts.getTime, Bytes.toBytes("normal"))
      put
    })


  }
}
