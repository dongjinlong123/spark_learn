package com.djl

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 远程调试
  */
object RemoteDebug {
  def main(args: Array[String]): Unit = {
    //集群模式
    val master:String ="spark://master:7077"
    //设置本地jar包的位置
    val localJarDir = "E:\\djl\\intellJIdea\\workspace\\spark_djl_01\\target\\spark_djl_01-1.0-SNAPSHOT.jar"

    val conf = new SparkConf().setAppName("RemoteDebug").setMaster(master).setJars(List(localJarDir))
    val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile("hdfs://master:9000/spark_learn/test.txt")
    val wd: RDD[Array[String]] = lines.map(_.split(" "))
    val c = wd.count()
    println(c)
    sc.stop()
  }
}
