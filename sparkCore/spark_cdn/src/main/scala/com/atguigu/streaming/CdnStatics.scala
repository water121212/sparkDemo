package com.atguigu.streaming

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.{DateTime, JodaTimePermission}
object CdnStatics {



  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("cdn").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val logs = sc.textFile("E:\\ideaWorkSpace\\sparkDemo\\sparkCore\\spark_cdn\\src\\main\\resources\\cdn.txt")
//    println("统计独立IP访问量前10位")
//    ipStatics(logs)
//    println("统计每个视频独立IP数")
//    videoIpStatics(logs)
    println("统计一天中每个小时间的流量")
    cdnHourLiuliang(logs)
    sc.stop()

//    val aa = "15/Feb/2017:00:00:46 +0800"
//    val bb =  getTime(aa)
//    println(bb)
  }
  //统计独立IP访问量前10位
  def ipStatics(data:RDD[String]):Unit={
    val logsArrary = data.map(_.split(" "))

    val ip2one =  logsArrary.map(x=>(x(0),1))                 //(ip,1)
    val ip2num = ip2one.reduceByKey(_+_)                      //(ip,n)
    val ipNums = ip2num.sortBy(_._2,false).take(10)
    ipNums.foreach(println(_))
  }
  //统计每个视频独立IP数
  def videoIpStatics(data: RDD[String]): Unit = {
    val logsArrary = data.map(_.split(" "))
    val filtered = logsArrary.filter(x=>x(6).endsWith(".mp4"))
    val mp42ipCount =  filtered.map(x=>(x(6)+"_"+x(0),1))    //
//    val mp4_ipCount= mp42ipCount.distinct()
//    val result =   mp4_ipCount.map{ x=> val param = x._1.split("_"); (param(0),x._2)}.reduceByKey(_+_)
    val result = mp42ipCount.groupByKey().map(x=>(x._1.split("_")(0),1)).reduceByKey(_+_)
    println("----------"+result.collect().length)
    result.foreach(println(_))
  }
  def cdnHourLiuliang(data: RDD[String]):Unit = {
    val logsArrary = data.map(_.split(" "))
    val time_num =  logsArrary.map(x=>(getTime(x(3)),x(9)))
    val result = time_num.reduceByKey(_+_)
    result.foreach(println(_))
  }

  def getTime(str:String):String={
    //day_hour
//    val datetime = new DateTime(str.toLong)
//    val time = datetime.getDayOfMonth+"_"+datetime.getHourOfDay
    str
  }
}
