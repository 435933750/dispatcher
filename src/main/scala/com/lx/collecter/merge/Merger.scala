package com.lx.collecter.merge

import com.alibaba.fastjson.JSON
import com.lx.collecter.merge.MergeLauncher.logger
import com.lx.util.{Constant, FsUtils, StringUtils}
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object Merger {

  def main(args: Array[String]): Unit = {
    val fileGroup = mutable.Map[String, ArrayBuffer[String]]()
    val json = JSON.parseObject(args(0))
    json.keySet().asScala.foreach(k => {
      val list = json.getJSONArray(k).toJavaList(classOf[String]).asScala
      fileGroup.put(k, ArrayBuffer(list: _*))
    })
    println("merge start")
    println(fileGroup.mkString("\n"))
    merge(fileGroup)
  }

  /**
    * 启动spark合并文件,在各层级都trycatch,保证分区之间,每个output之间异常不影响其他文件转存和合并
    * 通过在copyBytes的前后，rename之后，exception里面分别打印三次日志，可以找出已经合并成功，但是转存失败的文件。
    *
    * @param fileGroup
    */
  def merge(fileGroup: mutable.Map[String, ArrayBuffer[String]]): Unit = {
    //    val sparkConf = new SparkConf().setAppName("Merge").setMaster("local[20]")
    val sparkConf = new SparkConf().setAppName("Merge")
    val spark = new SparkContext(sparkConf)
    val rdd = spark.parallelize(fileGroup.toList).repartition(30)
    rdd.foreachPartition(p => {
      if (!p.isEmpty) {
        try {
          val fs = FsUtils()
          p.foreach(kv => {
            val compressPath = new Path(kv._1 + "/" + StringUtils.timestamp2String(System.currentTimeMillis(), "yyyyMMddHHmmssSSS"))
            val output = fs.create(compressPath)
            try {
              kv._2.foreach(f => {
                val fromPath = new Path(f)
                val toPath = new Path(f.replaceFirst(Constant.UNMERGE_PATH, Constant.MERGED_PATH))
                if (fs.exists(fromPath)) {
                  try {
                    logger.warn(s"meger begin from ${fromPath.toUri.getPath}")
                    val in = fs.open(fromPath)
                    //压缩,不关闭
                    fs.compressOrDecompress(in, output, fs.conf, true, false)
                    //                    IOUtils.copyBytes(in, output, fs.conf, false)
                    in.close()
                    logger.warn(s"compress success ${fromPath.toUri.getPath} to ${compressPath.toUri.getPath}")
                    if (!fs.exists(toPath.getParent)) {
                      fs.mkdirs(toPath.getParent)
                    }
                    logger.warn(s"mv ${fromPath.toString} to ${toPath.toString} :" + fs.rename(fromPath, toPath))
                  } catch {
                    case e: Exception => {
                      logger.error(s"merge error1:${fromPath.toUri.getPath} - ${toPath.toUri.getPath}", e)
                    }
                  }
                }
              })
            } catch {
              case e: Exception => {
                logger.error(s"merge error2:${compressPath.toUri.getPath}", e)
              }
            } finally {
              output.close()
            }
          })
        } catch {
          case e: Exception => {
            logger.error(s"merge error3", e)
          }
        }

      }
    })
    spark.stop()
  }
}
