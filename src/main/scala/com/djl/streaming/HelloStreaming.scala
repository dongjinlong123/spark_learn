package com.djl.streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * 滑动窗口案例
  *
  * 如果提示错误，就是checkpoint 的时候resource 目录的配置文件影响
  */
object HelloStreaming {
  def main(args: Array[String]): Unit = {
    //1. 创建配置
    val conf: SparkConf = new SparkConf().setAppName("HelloStreaming").setMaster("local[*]")

    //2. 每2秒钟做一个时间批次的处理
    val sc: StreamingContext = new StreamingContext(conf, Seconds(2))

    //3. 设置org.apache.spark 打印的日志级别，方便调试
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    val host: String = "192.168.72.100"
    val port: Int = 9999
    //4. 指定从哪儿读取数据，并将数据如何缓存
    val lines: ReceiverInputDStream[String] = sc.socketTextStream(host, port, StorageLevel.MEMORY_AND_DISK_SER)

    //5. 将获取的数据进行word count 统计
    //设置checkpoint 如果提示错误，就是resource 目录的配置文件影响
    sc.checkpoint("E:\\djl\\intellJIdea\\workspace\\spark_djl_01\\src\\main\\scala\\com\\djl\\streaming\\checkpoint")
    val wds: DStream[(String, Int)] = lines.flatMap(_.split(" ")).map((_, 1))
      //滑动窗口大小，保存30s的数据，必须是滑动时间间隔的倍数
      // .reduceByKeyAndWindow((a:Int,b:Int)=>a+b,Seconds(30))
      //每次处理保留上一次的状态，全量数据
      .updateStateByKey(addFunc)
    //6. 执行output算子
    wds.print()

    //7. sparkStreaming启动并且监听
    sc.start() //sparkStreaming开始
    sc.awaitTermination() //sparkStreaming监听
  }

  val addFunc = (curValues: Seq[Int], preValueState: Option[Int]) => {
    val curCount = curValues.sum
    val preCount = preValueState.getOrElse(0)
    Some(curCount + preCount)
  }
}
