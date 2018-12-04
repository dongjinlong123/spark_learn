package com.djl.streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 使用spark streaming 计算wordcount
  */
object wordCountStreaming {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("spark_streaming_demo")
    //每5秒钟做一个时间批次的处理
    val ssc = new StreamingContext(sparkConf,Seconds(5))
    //指定读取数据:内存和磁盘序列化化 args(0)：socket 的host   args(1)：socket 的 port
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream(args(0),args(1).toInt,StorageLevel.MEMORY_AND_DISK_SER)

    val wc: DStream[(String, Int)] = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    wc.print() //streaming 输出到终端
    //输出文件到hdfs上  ,参数 前缀 和 后缀
    wc.saveAsTextFiles("/spark_learn/spark_streaming/","djl")
    //启动
    ssc.start()
    //监听
    ssc.awaitTermination()

  }


}
