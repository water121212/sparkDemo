package com.atguigu.partitioner

import org.apache.spark.{Partitioner, SparkConf, SparkContext}
//自定义分区
class CuistomerPartitioner(numParts:Int) extends Partitioner{
  override def numPartitions = numParts

  override def getPartition(key: Any):Int = {
     val keyStr = key.toString
     keyStr.substring(keyStr.length-1).toInt%numParts
  }
}
object Test{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("partitioner").setMaster("local[*]")
    val sc = new SparkContext(conf)
   val data =  sc.makeRDD(List("aa.3","aa.4","aa.2","aa.4"))
    val result = data.map((_,1)).partitionBy(new CuistomerPartitioner(5))

   val result1 =  result.mapPartitionsWithIndex((index,items)=>Iterator(index+":"+items.mkString("|")))
    result1.collect().foreach(println(_))
    sc.stop()
  }
}