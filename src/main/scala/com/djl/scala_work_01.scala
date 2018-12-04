package com.djl

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 计算受欢迎老师
  */
object scala_work_01 {
  def main(args: Array[String]): Unit = {
    //数据http://oracle.test.data.com/dfh    oracle学科    dfh老师名
    //本地启动2个线程模拟
    val conf = new SparkConf().setAppName("homework")
    val sc  = new SparkContext(conf)
    val s1: RDD[String] = sc.textFile(args(0))
    //数据处理 (oracle,dfh) (学科,老师名)
    val s2 = s1.map(x=>{
      val str1: String = x.split("://")(1)
      val str2: String = str1.split("/")(0)
      val str3: String = str1.split("/")(1)
      val str4: String = str2.substring(0,str2.indexOf("."))
      (str4,str3)
    })
    //计算学科对应的老师数量((学科,老师),数量)
    val s3: RDD[((String, String), Int)] = s2.map((_,1)).reduceByKey(_+_)
    //计算得到(学科,(老师,数量)) 按学科进行分组
    val s4: RDD[(String, Iterable[((String, String), Int)])] = s3.groupBy(_._1._1)
    //二次排序 取前2条
    val ret: RDD[(String, List[((String, String), Int)])] = s4.mapValues(_.toList.sortBy(_._2).reverse.take(2))
    //输出
    ret.saveAsTextFile(args(1))
    //4. 释放资源
    sc.stop()
  }
}
