package com.lx.collecter.storage

import com.lx.collecter.dispatcher.ReceiveDispatcher
import com.lx.collecter.merge.FileName
import com.lx.util._
import org.apache.hadoop.fs.FSDataOutputStream
import org.apache.spark.broadcast.Broadcast
import org.slf4j.LoggerFactory

case class ReceiveStorage(tmpSuffix: String, pk: Broadcast[Array[Byte]]) extends ReceiveDispatcher {

  override val logger = LoggerFactory.getLogger(classOf[ReceiveStorage])


  /**
    * 清理数据
    *
    * @param value
    * @return (key:文件写出路径,value:文件内容)
    */
  override def clearData(value: Iterator[(String, String)]): Iterator[(String, String)] = {
    if (value.isEmpty) {
      value
    } else {
      value.map(v => {
        try {
          //step1.解密
          val bytes = ByteUtil.hexToByteArray(v._2)
          val cypher = bytes.splitAt(128)
          val random = Cypher.decrypt_rsa(cypher._1, pk.value)
          val message = new String(Cypher.decrypt_des(cypher._2, random))
          //step2.转换
          parseContent(message) match {
            case Some((time, path, content)) => {
              val key = s"${Constant.UNMERGE_PATH}${path}/${time.dropRight(2)}"
              val value = content.trim.substring(0, content.length - 1) + ",\"receiveTime\":\"" + time + "\"}"
              (key, value)
            }
            case None => logger.error(s"clearData error:${value}"); (Constant.ERROR_PATH, v._2)
          }
        } catch {
          case e: Exception => {
            logger.error(s"clearData error:${value}")
            (Constant.ERROR_PATH, v._2)
          }
        }
      })
    }
  }


  /**
    * 持久化数据
    * 一次调用是一个分区，一个分区是可能有多个key,每个key代表一个文件路径
    *
    * @param f
    * @param batchId 当前批次的id,用于给当前批次的文件后缀改为正式文件
    */
  override def persist(f: Iterator[(String, Iterable[String])], batchId: String): Unit = {
    if (!f.isEmpty) {
      var outputStream: FSDataOutputStream = null
      var fs: FsUtils = null
      try {
        fs = FsUtils()
        val now = System.nanoTime()
        f.foreach(iter => {
          val split = iter._1.splitAt(iter._1.lastIndexOf("/") + 1)
          val path = FileName.getFileName(split._2, batchId)
          outputStream = fs.create(split._1 + path)
          iter._2.foreach(line => {
            outputStream.write((s"${line}\n").getBytes("utf8"))
          })
          outputStream.flush()
        })
      } catch {
        case e: Exception => logger.error(s"persist data error: ${e.getMessage}"); throw new Exception(e)
      } finally {
        if (outputStream != null) outputStream.close()
        if (fs != null) fs.close()
      }
    }
  }

  def parseContent(content: String): Option[(String, String, String)] = {
    val commaIndex = content.indexOf(",", 15)
    if (commaIndex < 15) {
      logger.error("content format error")
      Some(StringUtils.timestamp2String(System.currentTimeMillis(), "yyyyMMddHHmmss").toString, "other", content)
    } else {
      val list = content.splitAt(commaIndex + 1)
      val displatherPath = list._1.dropRight(1)
      val splits = displatherPath.split(",")
      if (splits.length == 2) {
        Some(splits(0), splits(1), list._2)
      } else {
        logger.error("content format error")
        Some(StringUtils.timestamp2String(System.currentTimeMillis(), "yyyyMMddHHmmss").toString.toString, "other", content)
      }
    }
  }



}
