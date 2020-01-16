package com.lx.supplement.custom

import com.lx.supplement.bean.{LineBean, PersistBean}
import com.lx.util.{Constant, DateUtils}
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.util.Bytes

class CustomPayTime extends Custom {
  val sdf = DateUtils.getSdf()

  override def custom(lbs: List[LineBean], rus: Array[Result], config: (String, String)): List[Put] = {


    val ruMap = rus.map(r => (Bytes.toString(r.getRow), r)).toMap

    lbs.groupBy(_.pk).flatMap(row => {
      val minDate = row._2.minBy(lb => lb.data.getOrElse("create_time", "0000-00-00 00:00:00"))
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
        put.addColumn(Constant.SUPPLEMENT_FM, Bytes.toBytes(config._2), lb.ts.getTime, Bytes.toBytes(sdf.format(minDate.ts)))
        put
      })
    }).toList


  }
}
