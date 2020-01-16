package com.lx.supplement.custom
import com.lx.supplement.bean.LineBean
import com.lx.util.Constant
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.util.Bytes
import org.slf4j.LoggerFactory

class CustomPraise extends Custom {

  val logger = LoggerFactory.getLogger(classOf[CustomPraise])

  override def custom(lbs: List[LineBean], rus: Array[Result], config: (String, String)): List[Put] = {

    val grouped = lbs.map(lb => {
      val praiseUserId = lb.data.get("praise_user_id")
      val key = praiseUserId match {
        case Some(x) => x
        case None => ""
      }
      (key, lb)
    }).filter(_._1 != "")

    val ruMap = rus.map(r => (Bytes.toString(r.getRow), r)).toMap

    val reduced = grouped.groupBy(_._1).map(kv => {
      var oldValue = 0L
      try {
        ruMap.get(kv._1) match {
          case Some(x) => {
            val value = x.getColumnLatestCell(Constant.SUPPLEMENT_FM, Bytes.toBytes(config._2))
            if (value != null) {
              oldValue = Bytes.toString(value.getValue()).toLong
            }
          }
          case None =>
        }
      } catch {
        case e: Exception => logger.error("custom preise error", e)
      }
      val maxTs = kv._2.maxBy(_._2.ts.getTime)(Ordering.Long)._2.ts
      (kv._1, kv._2.size + oldValue, maxTs)
    })

    reduced.map(r => {
      val put = new Put(Bytes.toBytes(r._1))
      put.addColumn(Constant.SUPPLEMENT_FM, Bytes.toBytes(config._2), r._3.getTime, Bytes.toBytes(r._2.toString))
      put
    }).toList
  }

}
