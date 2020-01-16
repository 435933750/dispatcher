package com.lx.supplement.custom

import com.lx.supplement.bean.{LineBean, PersistBean}
import com.lx.util.Constant
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.util.Bytes
/**
  * 更新女神女角色字段
  */
class CustomGoddess extends Custom {
  override def custom(lbs: List[LineBean], rus: Array[Result], config: (String, String)): List[Put] = {
    lbs.map(lb => {
      val groupId = lb.data.get("group_id")
      val role = groupId match {
        case Some(x) => if ("0" != x) "wall_goddess" else "goddess"
        case None => "goddess"
      }
      val put = new Put(Bytes.toBytes(lb.pk))
      put.addColumn(Constant.SUPPLEMENT_FM, Bytes.toBytes(config._2), lb.ts.getTime, Bytes.toBytes(role))
      put
    })
  }
}
