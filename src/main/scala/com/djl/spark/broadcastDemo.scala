package com.djl.spark

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 两表join的时候，一个表数据不是很大的时候，
  * 如果在默认情况下会产生在shuffle，
  * 可以将小文件缓存在map，
  * 其实将小文件加载到内存中进行join ，
  * 模拟mr 中的map side join 。
  */
object broadcastDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("broadcastDemo").setMaster("local")
    val sc = new SparkContext(conf)
    //创建一个规则
    val rule = Map("cn" ->"中国","us"->"美国","jp"->"日本")

    //将driver 端的变量广播到属于自己的所有executor
    val broadcast: Broadcast[Map[String, String]] = sc.broadcast(rule)

    val lines: RDD[String] = sc.textFile("F:\\djl\\tmp\\join_data.txt")

    val ret: RDD[(String, String)] = lines.map(wd => {
      //在executor端task中引用driver端传输到executor中广播变量的数据broadcast
      val rule_bc: Map[String, String] = broadcast.value

      val wds: Array[String] = wd.split(",")
      val uname = wds(0)
      val country = rule_bc(wds(1))
      (uname, country)
    })

    println(ret.collect().toBuffer)
    sc.stop()
  }
}
