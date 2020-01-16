package com.lx.util

import java.text.SimpleDateFormat
import java.util.Date

import scala.collection.mutable

object DateUtils {

   private val sdf = mutable.Map[String, ThreadLocal[SimpleDateFormat]]()

  def getSdf(patten: String="yyyy-MM-dd HH:mm:ss"): SimpleDateFormat = {
    if (!sdf.contains(patten)) {
      synchronized {
        if (!sdf.contains(patten)) {
          sdf.put(patten, new ThreadLocal[SimpleDateFormat]() {
            override def initialValue(): SimpleDateFormat = {
              new SimpleDateFormat(patten)
            }
          })
        }
      }
    }
    sdf.get(patten).get.get()
  }

  def main(args: Array[String]): Unit = {
    println(getSdf().format(new Date(1578306857210L)))
  }
}
