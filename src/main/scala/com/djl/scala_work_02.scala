package com.djl

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable.HashMap

/**
  * 计算受欢迎老师
  */
object scala_work_02 {
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

    //统计所有学科
    val subAll: RDD[((String, String), Int)] = s2.map((_,1)).reduceByKey(_+_)
    //需要反复使用，则可以缓存
    subAll.cache()
    //统计有哪些学科，distinct除重
    val sub: Array[String] = subAll.map(_._1._1).distinct().collect()
    println(sub)
    //定义一个分区器 ,一个学科一个分区
    val subPartitioner: SubjectPartitioner = new SubjectPartitioner(sub)
    //数据整理
    val value: RDD[(String, (String, Int))] = subAll.map(x=>{(x._1._1,(x._1._2,x._2))})
    val partitioned: RDD[(String, (String, Int))] = value.partitionBy(subPartitioner)

    //分区进行操作排序取出前2条
    val ret: RDD[(String, (String, Int))] = partitioned.mapPartitions(_.toList.sortBy(_._2._2).reverse.take(2).iterator)

    println(ret.collect().toBuffer)

    //ret.saveAsTextFile(args(1))
    //4. 释放资源
    sc.stop()
  }
  class SubjectPartitioner(sub:Array[String]) extends Partitioner{
    //定义分区规则
    val rules = new HashMap[String,Int]()
    var i= 1
    for(s <- sub){
      rules +=(s -> i)
      i +=1
    }
    //有几个分区
    def numPartitions : scala.Int=sub.length + 1
    //根据传递的key决定该条数据到哪个分区
    def getPartition(key : scala.Any) : scala.Int={
      val k = key.toString//得到对应的key
      rules.getOrElse(k, 0) //返回对应的分区编号,如果key不存在则为第0个分区
    }
  }
}
