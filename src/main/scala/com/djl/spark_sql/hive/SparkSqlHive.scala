package com.djl.spark_sql.hive

import org.apache.spark.sql.SparkSession

/**
  * spark sql 操作hive
  */
object SparkSqlHive {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]")
      .appName("SparkSqlHive")
      //启动hive支持
      .enableHiveSupport()
      //hive存储数据在HDFS上的位置
      .config("spark.sql.warehouse.dir", "hdfs://master:9000/user/hive/warehouse")
      .getOrCreate()
    //执行hive-sql
    // spark.sql("show databases").show()
    // spark.sql("show tables").show()
    //创建自定义的udf
    spark.sql("create temporary function myfun as 'com.djl.hive.MyHiveUDF'")
    //使用udf 查询
    spark.sql(" select myfun(content) from djl_test ").show()
  }
}
