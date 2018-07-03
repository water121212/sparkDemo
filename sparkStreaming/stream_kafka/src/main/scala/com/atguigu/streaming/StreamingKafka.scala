package com.atguigu.streaming


import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo, TopicMetadataRequest}
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingKafka {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("kafka")
    val ssc = new StreamingContext(sparkConf,Seconds(5));

    //获取配置参数
    val brokerList = "hadoop101:9092,hadoop102:9092,hadoop103:9092"
    val zookeeper =  "hadoop101:2181,hadoop102:2181,hadoop103:2181"
    val sourceTopic = "source101"
    val targetTopic = "target101"
    val groupid = "water"

    //创建Kafka的连接参数
    val kafkaParam = Map[String,String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokerList,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.GROUP_ID_CONFIG -> groupid,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "largest"
    )

    var textKafkaDStream:InputDStream[(String,String)] = null
    //获取ZK中保存group + topic 的路径
    val topicDirs = new ZKGroupTopicDirs(groupid,sourceTopic)
    //最终保存Offset的地方
    val zkTopicPath = s"${topicDirs.consumerOffsetDir}"

    //zk客户端
    val zKClient = new ZkClient(zookeeper)
    val children = zKClient.countChildren(zkTopicPath)

    // 判ZK中是否有保存的数据
    if(children>0){
      //从上一个状态恢复
      //从ZK中获取Offset，最终保存上一次的状态  根据Offset来创建连接
      var fromOffsets:Map[TopicAndPartition, Long] = Map()

      //获取kafka集群的元信息
      val topicList = List(sourceTopic)
      val getLeaderConsumer = new SimpleConsumer("hadoop101",9092,100000,10000,"OffsetLookUp")
      val request = new TopicMetadataRequest(topicList,0)
      val response = getLeaderConsumer.send(request)
      val topicMetadataOption = response.topicsMetadata.headOption
      //partitions 就是包含了每一个分区的主节点所在的主机名称
      val partitions = topicMetadataOption match{
        case Some(tm) => tm.partitionsMetadata.map( pm=>(pm.partitionId,pm.leader.get.host)).toMap[Int,String]
        case None =>Map[Int,String]()
      }
      getLeaderConsumer.close()
      println("partition info is:"+partitions)
      println("children info is:"+children)
      println(s"${topicDirs.consumerOffsetDir}/")
     //获取每一个分区的最小offset
      for (i<-0 until  children){
         //先获得zk中第i个分区保存的offset
         val partitionOffset = zKClient.readData[String](s"${topicDirs.consumerOffsetDir}/${i}")
        println(s"Partition ${i} 目前offset是${partitionOffset}")
        //获取第i个分区的最小offset
        //创建一个到第i个分区所在的Host上的连接
         val consumerMin = new SimpleConsumer(partitions(i),9092,100000,10000,"getMinOffset")
        //创建一个请求
        val tp = TopicAndPartition(sourceTopic,i)
        val requestMin = OffsetRequest(Map(tp -> PartitionOffsetRequestInfo(OffsetRequest.EarliestTime,1)))
        //获取最小的offset
        val curOffsets = consumerMin.getOffsetsBefore(requestMin).partitionErrorAndOffsets(tp).offsets
        consumerMin.close()
        //校准
        var nextOffset = partitionOffset.toLong
        if(curOffsets.length>0 && nextOffset<curOffsets.head){
          nextOffset = curOffsets.head
        }
        fromOffsets +=(tp -> nextOffset)
        println(s"Partition ${i} 校准offset是${nextOffset}")
      }
      zKClient.close()

      println("从zk中恢复创建")
      val messageHandler = (mmd:MessageAndMetadata[String,String])=>(mmd.topic,mmd.message())
      textKafkaDStream = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder,(String,String)](ssc,kafkaParam,fromOffsets,messageHandler)
    }else{
      println("直接创建Kafka连接")
      textKafkaDStream = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParam,Set(sourceTopic))
    }
    //注意 需要先拿到新读取进来的数据的offset，不要转换成为另外一个Dstream后再拿去
    var offsetRanges = Array[OffsetRange]()
    val textKafkaDStream2 = textKafkaDStream.transform{rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }
    textKafkaDStream2.map(s=>"key:"+s._1+" value:"+s._2).foreachRDD{rdd=>
      rdd.foreachPartition{items=>
        //写回kafka
        //创建到kafka的连接
        val pool = KafkaConnPool(brokerList)
        val kafkaProxy =  pool.borrowObject()
       // 写数据
        for (item <- items){
          kafkaProxy.send(targetTopic,item)
        }
        pool.returnObject(kafkaProxy)
        //关闭连接
      }
      //需要将kafka每一个分区中读取的Offset更新到zk
      val updateTopicDirs = new ZKGroupTopicDirs(groupid,sourceTopic)
      val updateZkClient = new ZkClient(zookeeper)
      for(offset<-offsetRanges){
        //将读取的最新的Offset保存在ZK中
        println(s"Partition${offset.partition} 保存在zk中的数据offset是：${offset.fromOffset.toString}")
        val zkPath = s"${updateTopicDirs.consumerOffsetDir}/${offset.partition}"
        ZkUtils.updatePersistentPath(updateZkClient,zkPath,offset.fromOffset.toString)
      }
      updateZkClient.close()
    }
    //启动StreamingContext
    ssc.start()
    ssc.awaitTermination()
  }

}
