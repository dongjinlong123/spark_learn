package com.djl

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object Transformation {
  def main(args: Array[String]): Unit = {
    //创建sc
    val conf = new SparkConf().setAppName("test_transformation").setMaster("local[2]")
    val sc = new SparkContext(conf)
    //通过parallelize()创建rdd
    val arr = Array[String]("c","b","a","d")
    val rdd1 = sc.parallelize(arr)

    //sortBy排序
    val rdd2 = rdd1.sortBy(x=>x, false)


    //filter过滤
    val arr2 = Array[Int](1,3,5,3,2,21,31,4,1,1414,60)
    val rdd3 = sc.parallelize(arr2).filter(x=>x%2 == 1)//取奇数
    println(rdd3.collect.toBuffer)
    //flatMap压平
    val arr3=List(List("a b c","d w"),List("a b c","d w"),List("a b c","d w"))
    val rdd4 = sc.parallelize(arr3).flatMap(_.flatMap(_.split(" ")))
    println(rdd4.collect.toBuffer)
    //union并集 , intersection 差集
    var s1 = Array(1,3,4,5,7)
    var s2 = Array(2,4,6,7,8)
    val r1 = sc.parallelize(s1)
    val r2 = sc.parallelize(s2)
    val rdd5 = r1.union(r2)
    val rdd6 = r1.intersection(r2)
    //join 按照某个字段进行join ， key相同才可以join，leftOuterJoin 和 rightOuterJoin
    val j1 = List(("a",1),("b",2),("c",3))
    val j2 = List(("a","aaa"),("b","bbbb"),("d","dddd"))
    val rj1 =sc.parallelize(j1)
    val rj2 = sc.parallelize(j2)
    val rddj1 = rj1.join(rj2)
    val rddj2 = rj1.leftOuterJoin(rj2)
    val rddj3 = rj1.rightOuterJoin(rj2)
    //groupByKey
    val g1 = List(("a",1),("b",2),("a",3),("c",31),("d",32),("a",22))
    val rg1 = sc.parallelize(g1).groupByKey()

    //reduceByKey
    val rg4 = sc.parallelize(g1).reduceByKey(_+_)

    //全关联
    val st1 = sc.parallelize(List(("a",1),("b",2)))
    val st2 =  sc.parallelize(List(("a",1),("c",3)))
    val st3 = st1.cogroup(st2)
    //笛卡尔积
    val st4 = st1.cartesian(st2)
    

  }

  /**
    * 自定义分区器
    */
  class MyPartition extends org.apache.spark.Partitioner{
    override def numPartitions: Int = 3 //自定义分区数量
    override def getPartition(key: Any): Int = {
      //分区规则
      0
    }
  }
}
