package com.djl.spark

import java.sql.{Connection, DriverManager}


import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * 将数据库中的数据用RDD进行计算
  */
object JdbcRddDemo {
  val url: String = "jdbc:mysql://192.168.56.1:3306/db_test?useUnicode=true&characterEncoding=utf8"
  val driver: String = "com.mysql.jdbc.Driver"
  val username: String = "root"
  val password: String = "123456"

  def getConnect():Connection={
    DriverManager.getConnection(url, username, password)
  }
  def main(args: Array[String]): Unit = {
    val master = "spark://master:7077"
    val localJarDir = "E:\\djl\\intellJIdea\\workspace\\spark_djl_01\\target\\spark_djl_01-1.0-SNAPSHOT.jar"
    val conf = new SparkConf().setAppName("JdbcRddDemo").setMaster(master).setJars(List(localJarDir))
    val sc = new SparkContext(conf)


    Class.forName(driver)
    val sql = "select * from demo where id>= ? and id< ? "

    //2个分区得到的数据：ArrayBuffer((1,张三,123), (3,www,dasdasd), (4,aa,dadsa))
    // 缺失了一条数据（最好不要写< ）
    //1个分区得到的数据： ArrayBuffer((1,张三,123), (2,李四,aa), (3,www,dasdasd), (4,aa,dadsa))
    val partitions = 2//分区数
    //创建spark 的JdbcRDD
    val jdbcRdd = new JdbcRDD(sc,getConnect _,sql,0L,5L,partitions,rs=>{
        val id = rs.getString(1);
      val name = rs.getString(2)
      val value = rs.getString("value")
      (id,name,value)
    })
    val ret: mutable.Buffer[(String, String, String)] = jdbcRdd.collect().toBuffer
    println(ret)
    sc.stop()
  }
}
