package com.djl.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 查询IP在哪个城市
  */
object FindIpDemo {
  def main(args: Array[String]): Unit = {
    val ip = "125.213.100.123"

    //本地查询
    val conf = new SparkConf().setAppName("FindIpDemo").setMaster("local")
    val sc = new SparkContext(conf)
    val ipInfo: RDD[String] = sc.textFile("E:\\djl\\intellJIdea\\workspace\\spark_djl_01\\src\\main\\scala\\com\\djl\\spark\\ip.txt")
    //得到ip开始和结束以及城市
    val rules: RDD[(Long, Long, String)] = ipInfo.map(word => {
      val line: Array[String] = word.split("[|]")
      val start = line(2).toLong
      val end = line(3).toLong
      val city = line(6)
      (start, end, city)
    })
    println(rules.take(3).toBuffer)
    val lines: Array[(Long, Long, String)] = rules.collect()
    //二分法查询ip所在的位置
    val index: Int = IpCountDemo.binarySearch(lines,IpCountDemo.ipTOLong(ip))
    val city = lines(index)._3
    println(city)
  }
}
