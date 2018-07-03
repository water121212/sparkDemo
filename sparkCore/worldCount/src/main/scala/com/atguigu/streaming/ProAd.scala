package com.atguigu.streaming

import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime

object ProAd {
  def main(args: Array[String]): Unit = {
    //   new SparkConf().setAppName("worldCount").setMaster("local[*]")
//    val conf = new SparkConf().setAppName("worldCount").setJars(List()).setIfMissing("","")
    //创建sparkConf对象
    val conf = new SparkConf().setAppName("worldCount")
    //创建spark上下文对象
    val sc = new SparkContext(conf)
    //读取数据
    val textFile = sc.textFile("./RELEASE")

    //关闭连接
    sc.stop()
  }
  def getHour(timelong:String):String={
    val datetime = new DateTime(timelong.toLong)
    datetime.getHourOfDay
    ""
  }
}
