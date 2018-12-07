package com.djl.spark_sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * DataFrame的DLS风格 自身的API
  */
object DataFrameDLS {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DataFrameDLS").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val data = sc.parallelize(List("1,张三,22", "2,王者,33", "3,阿伦,21"))

    //    val rdd = data.map(x => {
    //      val lines = x.split("[,]")
    //      Teacher(lines(0).toInt, lines(1).toString, lines(2).toInt)
    //    })
    //不使用case class 的方式，使用createDataFrame
    val rowRDD: RDD[Row] = data.map(x => {
      val lines = x.split("[,]")
      Row(lines(0).toInt, lines(1).toString, lines(2).toInt)
    })


    val sqlContext = new SQLContext(sc)
    //导入隐式转换
    //import sqlContext.implicits._
    //val pdf = rdd.toDF()
    //使用createDataFrame
    val scheam = StructType(
      List(
        //定义字段名和类型以及是否可为空
        StructField("id", IntegerType, true),
        StructField("name", StringType, true),
        StructField("age", IntegerType, true)
      )
    )
    val pdf = sqlContext.createDataFrame(rowRDD, scheam)

    //DLS风格
    val result: Dataset[Row] = pdf.select("id", "name", "age").filter(pdf.col("age") > 21)
    result.show()
    //result.write.json("hdfs://master:9000/spark_learn/spark_data/spark_sql/out_data")
  }

}

case class Teacher(id: Int, name: String, age: Int)