package com.djl

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable.HashMap

/**
  * 计算受欢迎老师
  */
object scala_work_03 {
  def main(args: Array[String]): Unit = {
    //数据http://oracle.test.data.com/dfh    oracle学科    dfh老师名
    //本地启动2个线程模拟
    val conf = new SparkConf().setAppName("homework2").setMaster("local")
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
    //也可以写成这样但是效率低，会有两次shuffle reduceByKey  和 partitionBy
    //val reduceAndPar2: RDD[((String, String), Int)] = s2.map((_,1)).reduceByKey(_+_)
    //val ret: RDD[((String, String), Int)] = reduceAndPar2.partitionBy(subjectPar)

    //定义一个分区器 ,一个学科一个分区
    val subjectPar: SubjectPar = new SubjectPar
    //效率高就一次shuffle ，在reduceByKey的时候使用了自定义分区器
    val reduceAndPar: RDD[((String, String), Int)] = s2.map((_,1)).reduceByKey(subjectPar,_+_)

    //分区进行操作排序取出前2条
    val ret: RDD[((String, String), Int)] = reduceAndPar.mapPartitions(_.toList.sortBy(_._2).reverse.take(2).iterator)

    println(ret.collect().toBuffer)

    //ret.saveAsTextFile(args(1))
    //4. 释放资源
    sc.stop()
  }
  class SubjectPar extends Partitioner{
    //从数据库读取学科数目
    val rules = Map("java"->1,"oracle"->2)

    //有几个分区
    def numPartitions : scala.Int=2 + 1
    //根据传递的key决定该条数据到哪个分区
    def getPartition(key : scala.Any) : scala.Int={
      val k = key.asInstanceOf[Tuple2[String,String]]._1//得到对应的key
      rules.getOrElse(k, 0) //返回对应的分区编号,如果key不存在则为第0个分区
    }
  }
}
