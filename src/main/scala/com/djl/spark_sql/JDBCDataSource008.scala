package com.djl.spark_sql

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 读取数据库的数据
  */
object JDBCDataSource008 {
  def main(args: Array[String]): Unit = {
    //创建sparkSession
    val spark = SparkSession.builder().master("local[*]")
      .appName("JDBCDataSource008")
      .getOrCreate()

    //读取数据库数据
    val url: String = "jdbc:mysql://192.168.56.1:3306/db_test?useUnicode=true&characterEncoding=utf8"
    val driver: String = "com.mysql.jdbc.Driver"
    val username: String = "root"
    val password: String = "123456"
    val dbtable = "spark_demo"
    /**
      * 得到结构化数据
      */
    val jdbcDF: DataFrame = spark.read.format("jdbc").options(Map("url" -> url,
      "driver" -> driver, "dbtable" -> dbtable, "user" -> username, "password" -> password)).load()
    jdbcDF.show()


    //写入数据
    val p = new Properties()
    p.put("user", username)
    p.put("password", password)
    p.put("driver", driver)

    //mode常用append 和 overwrite
    jdbcDF.where(jdbcDF.col("id") <= 3).write.mode("append")
      .jdbc(url, dbtable, p)

  }
}
