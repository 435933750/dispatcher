package com.lx.supplement.custom

import com.lx.supplement.bean.LineBean
import com.lx.util.{Constant, HBaseUtil}
import org.apache.hadoop.hbase.client.{Mutation, Put, Result}
import org.apache.hadoop.hbase.util.Bytes

/**
  * 更新普通合作女角色字段
  */
class CustomSignatory extends Custom {
  override def custom(lbs: List[LineBean], rus: Array[Result], config: (String, String)): List[Put] = {
    lbs.map(lb => {
        val put = new Put(Bytes.toBytes(lb.pk))
        put.addColumn(Constant.SUPPLEMENT_FM, Bytes.toBytes(config._2), lb.ts.getTime, Bytes.toBytes("signatory"))
        put
    })
  }
}
