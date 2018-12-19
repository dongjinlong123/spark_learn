package com.djl.spark

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 应用场景：
  * 根据用户的ip区分用户所属的区域，
  * 来统计不同区域的访问量（需要将ip地址规则广播到每个executor中）
  */
object IpCountDemo {
  /** 将ip转化成10进制
    *
    * @param ip
    * @return
    */
  def ipTOLong(ip: String): Long = {
    val framents: Array[String] = ip.split("[.]")
    var ipNum = 0L
    for (i <- 0 until (framents.length)) {
      ipNum = framents(i).toLong | ipNum << 8L
    }
    ipNum
  }

  //二分法查找ip所在的位置
  def binarySearch(lines: Array[(Long, Long, String)], ip: Long): Int = {
    var start = 0
    var end = lines.length - 1
    var index = -1 // 找不到默认index为-1
    while (start <= end) {
      var middle = (start + end) / 2
      //计算中间位置
      if (ip >= (lines(middle)._1) && ip <= (lines(middle)._2)) {
        return middle
      }
      if (ip > (lines(middle)._2)) {
        start = middle + 1
      }
      if (ip < (lines(middle)._1)) {
        end = middle - 1
      }
    }
    index
  }

  def main(args: Array[String]): Unit = {

    //idea 远程调试集群模式yarn on client
    val master = "spark://master:7077"
    // ip地址规则文件
    val ipRuleFile = "hdfs://master:9000/spark_learn/ip_demo/ip.txt"
    val access_log_file = "hdfs://master:9000/spark_learn/ip_demo/access.log"
    val localJarDir = "E:\\djl\\intellJIdea\\workspace\\spark_djl_01\\target\\spark_djl_01-1.0-SNAPSHOT.jar"

    val conf = new SparkConf().setAppName("IpCountDemo").setMaster(master).setJars(List(localJarDir))
    val sc = new SparkContext(conf)

    //解析ip地址规则文件，创建一个规则
    //数据：1.0.1.0|1.0.3.255|16777472|16778239|亚洲|中国|福建|福州||电信|350100|China|CN|119.306239|26.075302
    val ipInfo: RDD[String] = sc.textFile(ipRuleFile)
    val rules: RDD[(Long, Long, String)] = ipInfo.map(word => {
      val line: Array[String] = word.split("[|]")
      val start = line(2).toLong
      val end = line(3).toLong
      val city = line(6)
      (start, end, city)
    })
    //数据收集到driver端(广播之前必须要收集到driver端,因为在executor中可能只有该文件的一部分)
    val driver_rules: Array[(Long, Long, String)] = rules.collect()
    //driver端广播ip规则到属于自己的executor中
    val broadcast: Broadcast[Array[(Long, Long, String)]] = sc.broadcast(driver_rules)

    //读取日志文件(第二条数据为IP地址)
    val log: RDD[String] = sc.textFile(access_log_file)
    val result: RDD[(String, Int)] = log.map(x => {
      val arr: Array[String] = x.split("[|]")
      val ip = arr(1)
      //得到广播变量
      val bc: Array[(Long, Long, String)] = broadcast.value
      //二分法查找所在的位置
      val index = binarySearch(bc, ipTOLong(ip))
      val city: String = bc(index)._3
      (city, 1)
    }).reduceByKey(_ + _)

    //println(result.toBuffer)
    //将数据存储到数据库中 , 在executor端将数据写入到数据库中
    //result.foreach(wd => {
    //一条条去处理，写入mysql ，这样不可行。每写一条创建一个JDBC连接，消耗太多资源
    // })

    //一个分区处理，一个分区就是一个迭代器
    result.foreachPartition(part => {
      //传入一个分区过去，一个分区有多条数据，一个分区创建一个JDBC连接，写完分区数据再关闭连接
      data2MySql(part)
    })
    println("数据写入数据库完成===============")
    sc.stop()
  }

  val url: String = "jdbc:mysql://192.168.56.1:3306/db_test?useUnicode=true&characterEncoding=utf8"
  val driver: String = "com.mysql.jdbc.Driver"
  val username: String = "root"
  val password: String = "123456"

  def data2MySql(part: Iterator[(String, Int)]): Unit = {
    Class.forName(driver)
    val connection: Connection = DriverManager.getConnection(url, username, password)
    part.foreach(data => {
      val statement: PreparedStatement = connection.prepareStatement("insert into demo(name,value) values(?,?)")
      statement.setString(1, data._1)
      statement.setString(2, data._2.toString)
      statement.execute()
    })
    connection.close()
  }
}
