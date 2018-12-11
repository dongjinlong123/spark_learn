package com.djl.spark_sql


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * spark2.0 中的DataSet
  *
  * 全局只需要一个session ， 可以不close ，因为创建的时候getOrCreate()
  * 可以实现反复的提交任务
  * 现在的DataFrame 是DataSet[Row] 的类型 spark1.6 之后新的分布式数据集
  * 比RDD性能更高。底层还是RDD ， 执行之前会对RDD进行优化
  *
  */
object HelloDataSet004 {
  def main(args: Array[String]): Unit = {
    //新API  创建sparkSession
    val session = SparkSession.builder().appName("HelloDataSet")
      .master("local[*]").getOrCreate()


    //通过sparkSession 获得sparkContext 创建RDD
    val rdd: RDD[String] = session.sparkContext.textFile("./spark_sql.txt")
    //rdd 转化为RDD[Row]
    val rowRDD: RDD[Row] = rdd.map(x => {
      val lines = x.split("[,]")
      Row(lines(0).toInt, lines(1), lines(2).toInt)
    })

    val schema = StructType(List(
      StructField("id", IntegerType, true),
      StructField("name", StringType, true),
      StructField("age", IntegerType, true)
    ))
      //通过sparkSession 的createDataFrame
      val df: DataFrame = session.createDataFrame(rowRDD, schema)
    // df.registerTempTable("djl_test") //注册临时表，方法已过期

    // 通过df 进行视图的创建
    //df.createGlobalTempView("djl_test") //创建一个全局的视图
    df.createTempView("djl_test") //创建一个临时的视图

    //调用sparkSession的方法，执行sql语句
    val ret: DataFrame = session.sql("select * from djl_test")
    ret.show()

  }
}
