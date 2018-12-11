package com.djl.spark_sql.udf

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * spark-sql 计算IP归属地
  */
object SQLIPLocation001 {
  def main(args: Array[String]): Unit = {
    //创建sparkSession
    val spark = SparkSession.builder().appName("SQLIPLocation")
      .master("local[*]").getOrCreate()

    //导入隐式转换
    import spark.implicits._
    //读取数据 ip规则
    val ruleLines: Dataset[String] = spark.read.textFile(args(0))
    //1.0.1.0|1.0.3.255|16777472|16778239|亚洲|中国|福建|福州||电信|350100|China|CN|119.306239|26.075302
    //对ip规则进行整理
    val ruleDfs: Dataset[(Long, Long, String)] = ruleLines.map(x => {
      val fields: Array[String] = x.split("[|]")
      val ipStart = fields(2).toLong
      val ipEnd = fields(3).toLong
      val province = fields(6).toString
      (ipStart, ipEnd, province)
    })
    //指定schema 信息,
    val df: DataFrame = ruleDfs.toDF("start_num", "end_num", "province")
    //注册视图
    df.createTempView("ip_rules")

    //读取访问日志
    val log: Dataset[String] = spark.read.textFile(args(0))
    val ipDF: Dataset[String] = log.map(x => {
      val fields = x.split("[|]")
      val ip = fields(1)
      ip
    })
    val logDf = ipDF.toDF("ip")
    //注册视图
    logDf.createTempView("ip_v")

    //注册自定义函数
    spark.udf.register("ip2Long", ipToLong)
    //使用自定义函数ip2Long(str)
    //两个大表join 效率太慢
    val ret: DataFrame = spark.sql("select t2.province,count(*) cn from ip_v t1 join ip_rules t2 " +
      "on (ip2Long(t1.ip) >= t2.start_num and ip2Long(t1.ip) <= t2.end_num) group by t2.province")
    ret.show()

  }

  /**
    * 字符串ip转化成十进制 Long类型
    */
  val ipToLong: (String) => Long = { (x) => {
    val arrs: Array[String] = x.split("[.]")
    var ipNum = 0L
    for (i <- 0 until (arrs.length)) {
      ipNum = arrs(i).toLong | ipNum << 8L
    }
    ipNum
  }
  }
}
