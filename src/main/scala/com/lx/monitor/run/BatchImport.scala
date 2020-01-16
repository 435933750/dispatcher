package com.lx.monitor.run

import java.util.{Date, Properties}

import com.lx.util.{Constant, DateUtils}
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

//不cache 20mins
object BatchImport {

  def main(args: Array[String]): Unit = {

    val prop = new Properties()
    prop.setProperty("user", Constant.JDBC_USER_NAME)
    prop.setProperty("password", Constant.JDBC_PASS_WORD)
    prop.setProperty("driver", Constant.JDBC_DRIVER)

    val list = List(("",""))

    val spark = SparkSession.builder().appName("ImportMonitorData").enableHiveSupport().getOrCreate()
    spark.sql("select * from real_time_mysql_data.alarm_scheme_minute WHERE create_time < '2020-01-07 13:00:00'").cache().createOrReplaceTempView("tb")
    list.foreach({ case (sql, table) => {
      spark.sql(sql).write.mode(SaveMode.Append).jdbc(Constant.JDBC_URL, table, prop)
    }
    })

  }


  def getSsc(): StreamingContext = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("a")
    new StreamingContext(conf, Seconds(10))
  }

  def getSc(): SparkContext = {
    val conf = new SparkConf().setMaster("local").setAppName("a")
    new SparkContext(conf)
  }

  def getSs(): SparkSession = {
    val conf = new SparkConf().setMaster("local").setAppName("a")
    SparkSession.builder().master("local").appName("a").getOrCreate()
  }
}
