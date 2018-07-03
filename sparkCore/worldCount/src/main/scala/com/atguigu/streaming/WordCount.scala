package com.atguigu.streaming


import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
      val conf =  new SparkConf().setAppName("worldCount").setMaster("local[*]")
//    val conf = new SparkConf().setAppName("worldCount").setJars(List()).setIfMissing("","")
    //创建sparkConf对象
//    val conf = new SparkConf().setAppName("worldCount")
    //创建spark上下文对象
    val sc = new SparkContext(conf)
    //读取数据
    val textFile = sc.textFile("./RELEASE.txt")
    //按空格切分
    val worlds =  textFile.flatMap(_.split(" "))
    //转换为kv格式
    val k2v = worlds.map((_,1))

    //将相同的key合并
    val result = k2v.reduceByKey(_+_)
    //输出结果
    result.collect().foreach(println( _))
    //关闭连接
    sc.stop()
  }
}
