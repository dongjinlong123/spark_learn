package com.djl.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 两表join的时候，一个表数据不是很大的时候，
  * 如果在默认情况下会产生在shuffle，
  * 可以将小文件缓存在map，
  * 其实将小文件加载到内存中进行join ，
  * 模拟mr 中的map side join 。
  */
object MapSideJoin {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("map_side_join").setMaster("local")
    val sc = new SparkContext(conf)
    //创建一个规则，如果在driver端创建的一个变量，并且传入到RDD的方法函数中
    //RDD方法的函数是在executor的task中执行的，
    //driver 会将这个变量发送给每一个task，造成大量网络传输
    //造成任务执行很慢，spark提供了一种机制广播变量
    val rule = Map("cn" ->"中国","us"->"美国","jp"->"日本")
    val lines: RDD[String] = sc.textFile("F:\\djl\\tmp\\join_data.txt")

    val ret: RDD[(String, String)] = lines.map(wd => {
      //真正计算的时候避免这种情况，函数执行在executor
      val wds: Array[String] = wd.split(",")
      val uname = wds(0)
      val country = rule(wds(1))
      (uname, country)
    })

    println(ret.collect().toBuffer)
    sc.stop()
  }
}
