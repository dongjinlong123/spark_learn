package com.djl

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Partitioner, SparkConf, SparkContext}



/**
  * 计算受欢迎老师
  */
object scala_work_04 {
  def main(args: Array[String]): Unit = {

    //数据http://oracle.test.data.com/dfh    oracle学科    dfh老师名
    //本地启动2个线程模拟
    val conf = new SparkConf().setAppName("homework2").setMaster("local")
    val sc  = new SparkContext(conf)

    val s1: RDD[String] = sc.textFile(args(0))
    //数据处理 (oracle,dfh) (学科,老师名)
    var subList = List("java","php","oracle","hadoop","linux")
    val s2 = s1.map(x=>{
      val str1: String = x.split("://")(1)
      val str2: String = str1.split("/")(0)
      val str3: String = str1.split("/")(1)
      val str4: String = str2.substring(0,str2.indexOf("."))
      (str4,str3)
    }).persist(StorageLevel.MEMORY_ONLY_SER)//.cache() //内存足够大。只需要读一次
    //不使用List的排序方式使用RDD的排序方式
    //过滤出对应的学科，然后使用RDD的排序方法进行排序
    //提交多次到集群,进行结果计算，数据量小聚合非常快
    for (sub <- subList){
      println(sub)
      val filtered: RDD[(String, String)] = s2.filter(s=>sub.equals(s._1))
      val ret: RDD[((String, String), Int)] = filtered.map((_,1)).reduceByKey(_+_)
      //调用rdd 的sortBy 方法不会在内存中排，而是在磁盘上
      val ret2: Array[((String, String), Int)] = ret.sortBy(_._2,false).take(2)
      println(ret2.toBuffer)
    }
    //ret.saveAsTextFile(args(1))
    //4. 释放资源
    sc.stop()
  }
}
