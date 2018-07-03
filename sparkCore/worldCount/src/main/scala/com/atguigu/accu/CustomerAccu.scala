package com.atguigu.accu

import java.util

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.util.AccumulatorV2

class CustomerAccu extends AccumulatorV2[String,util.Set[String]]{
  // 定义一个累加器的内存结构，用于保存带有字母的字符串。
  val _logArray:util.Set[String] = new util.HashSet[String]()
  // 检测累加器内部数据结构是否为空。
  override def isZero: Boolean = {
    _logArray.isEmpty
  }

  override def reset(): Unit = {
    _logArray.clear()
  }

  override def add(v: String): Unit = {
    _logArray.add(v)
  }

  override def value: util.Set[String] = {
    java.util.Collections.unmodifiableSet(_logArray)
  }
  // 提供将多个分区的累加器的值进行合并的操作函数
  override def merge(other: AccumulatorV2[String, util.Set[String]]): Unit = {
    // 通过类型检测将o这个累加器的值加入到当前_logArray结构中
    other match {
      case o:CustomerAccu =>_logArray.addAll(o.value)
    }
  }
  override def copy(): AccumulatorV2[String, util.Set[String]] = {
    val newAcc = new CustomerAccu()
    newAcc.synchronized{
      newAcc._logArray.addAll(_logArray)
    }
    newAcc
  }
}
// 过滤掉带字母的
object Test{
    def main(args: Array[String]): Unit = {
       val conf = new SparkConf().setAppName("accumulator").setMaster("local[*]")
       val sc = new SparkContext(conf)
       val accum = new CustomerAccu()
       sc.register(accum)
       val sum = sc.parallelize(Array("1", "2a", "3", "4b", "5", "6", "7cd", "8", "9"), 2).filter(line=>{
         val pattern = """^-?(\d+)"""
         val flag = line.matches(pattern)
         if (!flag) {
           accum.add(line)
         }
          flag
         }).map(_.toInt).reduce(_+_)
      println("sum: " + sum)
        accum.value
//      for (v <- accum.value)
//        print(v + "")
//      println()
      sc.stop()
    }

}