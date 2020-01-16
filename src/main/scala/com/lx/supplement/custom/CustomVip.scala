package com.lx.supplement.custom

import com.lx.supplement.bean.{LineBean, PersistBean}
import com.lx.util.Constant
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.util.Bytes
import org.slf4j.LoggerFactory

class CustomVip extends Custom {
  val logger = LoggerFactory.getLogger(classOf[CustomVip])


  override def custom(lbs: List[LineBean], rus: Array[Result], config: (String, String)): List[Put] = {
    //    case when pay_status=0 then 'free'
    //    when pay_status!=0 and pay_status!=1 then 'other'
    //    when if(money is NULL ,0,money)<1 then 'C'
    //    when if(money is NULL ,0,money)<10 then 'B'
    //    else 'A' end as vip_level,
    lbs.map(lb => {
      var level = lb.data.get("pay_status") match {
        case Some(x) => {
          if ("0" == x) "free"
          else if ("1" != x) "other"
          else ""
        }
        case None => ""
      }

      if ("" == level) {
        try {
          val money = lb.data.getOrElse("money", 0.0).toString.toDouble
          if (money < 1) level = "C"
          else if (money < 10) level = "B"
          else level = "A"
        } catch {
          case e: Exception => logger.error(s"transfor money error:${lb.pk}", e); level = "other"
        }
      }
      val put = new Put(Bytes.toBytes(lb.pk))
      put.addColumn(Constant.SUPPLEMENT_FM, Bytes.toBytes(config._2), lb.ts.getTime, Bytes.toBytes(level))
      put
    })
  }
}
