package com.lx.supplement.custom

import com.lx.supplement.bean.LineBean
import com.lx.util.{Constant, DateUtils}
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.util.Bytes

class CustomPayGoldSum extends Custom {
  val sdf = DateUtils.getSdf()

  override def custom(lbs: List[LineBean], rus: Array[Result], config: (String, String)): List[Put] = {


    val ruMap = rus.map(r => (Bytes.toString(r.getRow), r)).toMap

   /* lbs.groupBy(_.pk).flatMap(row => {
      var sum = 0.0
      row._2.foreach(lb=>{
        val vs = lb.data.get("verify_result")
        val add = vs match {
          case Some(x) => x == "1"
          case None => false
        }
        if(add){
          sum+=lb.data.get("verify_result")
        }
      })
    })*/

    Nil


    /*lbs.groupBy(_.pk).flatMap(row => {
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
    }).toList*/


  }
}
