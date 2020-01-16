package com.lx

import com.lx.util.OffsetManager
import org.apache.spark.streaming.dstream.InputDStream

trait Dispatcher[K, V, CI, CO, P] extends Serializable {

  /**
    * 处理数据入口
    *
    * @param ds
    */
  def handleData(ds: InputDStream[(K, V)], kakfaOfs: OffsetManager, ctx: RuntimeContext)

  /**
    * 清理转换数据
    *
    * @param value
    * @return
    */
  def clearData(value: CI): CO

  /**
    * 持久化数据
    *
    * @param f
    */
  def persist(f:P,batchId:String)

}
