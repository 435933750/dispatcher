package com.lx.supplement.custom

import com.lx.supplement.bean.{LineBean, PersistBean}
import org.apache.hadoop.hbase.client.{Mutation, Put, Result}

trait Custom {
  def custom(lbs: List[LineBean], rus: Array[Result], config: (String, String)): List[Put]
}
