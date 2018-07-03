package com.atguigu.streaming

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

class CustomerReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {

  //接收器启动的时候
  override def onStart(): Unit = {
    new Thread("socket Receiver") {
      override def run(): Unit = {
        recevice()
      }
    }
  }

  def recevice(): Unit = {
    var socket: Socket = null
    var input: String = null
    try {
      socket = new Socket(host, port)
      val reader = new BufferedReader(new InputStreamReader(socket.getInputStream, StandardCharsets.UTF_8))
      input = reader.readLine()
      while (!isStopped() && input != null) {
        store(input) //数据提交给框架
        input = reader.readLine()
      }
    } catch {
      case e: java.net.ConnectException => restart("restart")
    }
  }

  //接收器停止的时候，主要做资源的销毁
  override def onStop(): Unit = {

  }
}

object WorldCount {

  def main(args: Array[String]): Unit = {
    //创建SparkConf对象
    val conf = new SparkConf().setAppName("count").setMaster("local[*]")
    //创建StreamingContext对象
    val ssc = new StreamingContext(conf, Seconds(5))
    //创建一个接收器
    //    val linesDStream = ssc.socketTextStream("hadoop101",9999)
    val linesDStream = ssc.receiverStream(new CustomerReceiver("hadoop101", 9999))
    val worldsDStream = linesDStream.flatMap(_.split(" "))
    val kvDStream = worldsDStream.map((_, 1))
    val result = kvDStream.reduceByKey(_ + _)
    result.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
