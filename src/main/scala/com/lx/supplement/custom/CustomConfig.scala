package com.lx.supplement.custom

import com.lx.supplement.bean.{LineBean, PersistBean, ReceiveBean, SupplementBean}
import org.apache.hadoop.hbase.client.{Put, Result}
import org.slf4j.LoggerFactory

import scala.collection.mutable

object CustomConfig {

  val logger = LoggerFactory.getLogger(CustomConfig.getClass)

  def custom(lbs: List[LineBean], rus: Array[Result], configMap: mutable.Map[String, String]): List[Put] = {
    configMap.map(config => {
      try {
        val clz = Class.forName(config._1.split("\\|")(1))
        clz.getDeclaredMethod("custom", classOf[List[LineBean]], classOf[Array[Result]], classOf[(String, String)])
          .invoke(clz.newInstance(), lbs, rus, config)
          .asInstanceOf[List[Put]]
      } catch {
        case e: Exception => logger.error("reflact error", e); Nil
      }
    }).filter(!_.isEmpty).flatten.toList
  }

}
