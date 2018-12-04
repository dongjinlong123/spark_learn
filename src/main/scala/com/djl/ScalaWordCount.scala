package com.djl

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ScalaWordCount {
  def main(args: Array[String]): Unit = {
   // System.setProperty("hadoop.home.dir", "E:\\djl\\hadoop_window\\hadoop-common-2.6.0")
    //1. 创建SparkConf 并配置信息
    val conf = new SparkConf().setAppName("my_first_spark_app")//.setMaster("local")
    //2. 创建sc, sc是sparkContext，spark程序执行入口
    val sc = new SparkContext(conf)
    //3. 编写spark程序
    // scala 中的flatMap 和 spark中的flatMap名字作用一样，但底层实现不一样
    // 一个单机执行计算数组数据。一个是集群中执行计算RDD数据
   // sc.textFile(args(0)).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).sortBy(_._2,false).saveAsTextFile(args(1))
    val lines: RDD[String] = sc.textFile(args(0)) //指定从哪里读取数据 RDD分布式数据集
    //将一行内容进行切分压平
    val words: RDD[String] = lines.flatMap(_.split(" "))
    //将单词和1 放入元组中
    val wordAndOne: RDD[(String, Int)] = words.map((_,1))
    //继续聚合
    val reduce: RDD[(String, Int)] = wordAndOne.reduceByKey(_+_)
    //排序false 降序  true 升序
    val ret: RDD[(String, Int)] = reduce.sortBy(_._2,false)
    //输出
    ret.saveAsTextFile(args(1))
    //4. 释放资源
    sc.stop()

  }
}
