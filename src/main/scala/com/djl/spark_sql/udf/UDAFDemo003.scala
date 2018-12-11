package com.djl.spark_sql.udf

import java.lang

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
  * 自定义UDAF需要继承UserDefinedAggregateFunction 类
  */
class GeoMean extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = {
    //输入参数类型
    StructType(List(StructField("value",DoubleType)))
  }

  override def bufferSchema: StructType = {
    //定义存储聚合运算时产生的中间结果的schema
    StructType(List(
      StructField("count", LongType),
      StructField("produce", DoubleType)

    ))
  }

  //标明UDAF返回类型
  override def dataType: DataType = DoubleType

  //用户标记针对给定的一组输入，UDAF是否总是生成相同的记过
  override def deterministic: Boolean = true

  //对聚合运算中间结果初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L //数据累加
    buffer(1) = 1.0 //数据累乘
  }

  //分区中数据单独计算，每处理一条数据都要执行update
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[Long](0) + 1
    //初始的数据 * 传递的数据
    buffer(1) = buffer.getAs[Double](1) * input.getAs[Double](0)
  }

  //分布式计算，分区聚合， 分区结果进行运算
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Long](0) + buffer2.getAs[Long](0)
    buffer1(1) = buffer1.getAs[Double](1) * buffer2.getAs[Double](1)
  }

  //完成对聚合buffer值的运算，得到最后的结果
  override def evaluate(buffer: Row): Any = {
    //求集合平均数  5开根号(a1*a2*a3*a4*a5)
    math.pow(buffer.getDouble(1), 1.toDouble / buffer.getLong(0))
  }
}

object UDAFDemo003 {
  def main(args: Array[String]): Unit = {
    var r = math.pow(1 * 2 * 3 * 4 * 5 * 6 * 7 * 8 * 9, 1.toDouble / 9)
    println("得到的结果:" + r )//得到的结果:4.147166274396913

    //分布式计算
    val spark = SparkSession.builder().appName("UDAFDemo003")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._
    //创建DataFrame
    val ds: Dataset[lang.Long] = spark.range(1, 10)

    ds.createTempView("v_num")
    val gm = new GeoMean()
    //注册udf
    spark.udf.register("gm",gm)
    //执行sql
    spark.sql("select gm(id) as gm from v_num").show()
  }

}
