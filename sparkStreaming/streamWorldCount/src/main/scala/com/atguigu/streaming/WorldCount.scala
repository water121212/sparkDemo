package com.atguigu.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WorldCount {


  def main(args: Array[String]): Unit = {
    //创建SparkConf对象
val conf = new SparkConf().setAppName("count").setMaster("local[*]")
    //创建StreamingContext对象
    val ssc = new StreamingContext(conf,Seconds(5))
    //创建一个接收器
//    val customReceiverStream = ssc.receiverStream(new CustomerReceiver("hadoop101", 9999))
    val linesDStream = ssc.socketTextStream("hadoop101",9999)
    val worldsDStream =  linesDStream.flatMap(_.split(" "))
    val kvDStream =  worldsDStream.map((_,1))
    val result = kvDStream.reduceByKey(_+_)
    result.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
