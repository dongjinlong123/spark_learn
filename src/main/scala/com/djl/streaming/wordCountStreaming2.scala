package com.djl.streaming


import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf}

/**
  * 使用spark streaming 计算wordcount
  * 每次处理保留上一次的状态
  */
object wordCountStreaming2 {
  // string 代表 传输的tuple 中的 key ，（存在reduce） seq[Int]相当于value ，
  // Option[Int] 可选元素，状态的记忆 ,存储的就是之前的值
  def updateFun =(it:Iterable[(String,Seq[Int],Option[Int])])=>{
    it.map(t =>(t._1,t._2.sum+t._3.getOrElse(0)))
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("spark_streaming_demo")
    //每5秒钟做一个时间批次的处理
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    ssc.checkpoint("./local_checkpoint")
    //指定读取数据:内存和磁盘序列化化 args(0)：socket 的host   args(1)：socket 的 port
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream(args(0),args(1).toInt,StorageLevel.MEMORY_AND_DISK_SER)


    //val wc: DStream[(String, Int)] = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    val words: DStream[String] = lines.flatMap(_.split(" "))
    //参数1 ： 更新参数  ， 参数2 ： 自定义partition ， 参数3 ： 是否记忆，是否更新
val partitioner: HashPartitioner = new HashPartitioner(ssc.sparkContext.defaultParallelism)
  // val wc = words.map(x=>(x,1)).updateStateByKey(updateFun,partitioner,true)

    //wc.print() //streaming 输出到终端
    //输出文件到hdfs上  ,参数 前缀 和 后缀
    //wc.saveAsTextFiles("/spark_learn/spark_streaming2/","djl")
    //启动
    ssc.start()
    //监听
    ssc.awaitTermination()

  }


}
