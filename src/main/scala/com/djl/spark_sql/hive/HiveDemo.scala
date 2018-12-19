package com.djl.spark_sql.hive

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
  * spark-hive的作业
  */
object HiveDemo {


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("HiveDemo")
      .master("local[*]").enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    //得到orders  和 priors 的DataFrame
    val orders: DataFrame = spark.sql("select * from orders")
    val priors: DataFrame = spark.sql("select * from order_products_prior")
    //Feat(priors,orders)
    userFeat(priors, orders)
  }

  /**
    * product 统计/特征（3个特征）
    * - 统计product被购买的数据量
    * - 统计product被reordered的数量（再次购买）
    * - 结合上面数量统计product购买的reordered的比率
    */
  def Feat(priors: DataFrame, orders: DataFrame): Unit = {
    //统计product被购买的数据量
    priors.groupBy("product_id").count().limit(10).show()

    //统计product被reordered的数量（再次购买）
    //结合上面数量统计product购买的reordered的比率
    val ret: DataFrame = priors.selectExpr("product_id", "cast(reordered as int)")
      .groupBy("product_id").agg(sum("reordered").as("prod_sum_rod"),
      avg("reordered").as("prod_rod_rate")).limit(10)
    ret.show()
  }

  /**
    * - 每个用户平均购买订单的间隔周期
    * - 每个用户的总订单数量
    * - 每个用户购买的product商品去重后的集合数据
    * - 每个用户总商品数量以及去重后的商品数量
    * - 每个用户购买的平均每个订单的商品数量（hive已做 过）
    */
  def userFeat(priors: DataFrame, orders: DataFrame): Unit = {
    //1. 每个用户平均购买订单的间隔周期
    //异常值处理，将days_since_prior_order 中的空值进行处理，新增为一列，拼接到表后
    val newOrder = orders.selectExpr("*", "if(days_since_prior_order='',0,days_since_prior_order) as dspo")
      .drop("days_since_prior_order") //删除掉某一列数据
    newOrder.selectExpr("user_id", "cast(dspo as int)").groupBy("user_id").agg(avg("dspo"))
      .withColumnRenamed("avg(dspo)", "u_avg_day_gap") //修改名称的另一种方法
      .limit(10).show()
    //2. 每个用户总订单量
    orders.groupBy("user_id").count().limit(10).show()

    //3.每个用户购买的product商品去重后的集合数据
    //两表进行join
    val joinTB: DataFrame = orders.join(priors, "order_id").select("user_id", "product_id")
    import priors.sparkSession.implicits._
    //将DF转换成rdd 进行操作，数据处理
    val ret: DataFrame = joinTB.rdd.map(x => (x(0).toString, x(1).toString)).groupByKey()
      .mapValues(_.toSet.mkString(",")).toDF("user_id", "product_recodes")
    ret.limit(10).show()
    //每个用户总商品数量以及去重后的商品数量
    val pro_cnt = joinTB.groupBy("user_id")
      .agg(count("product_id").as("pro_cnt"),
        countDistinct("product_id").as("pro_dis_cnt"))
    pro_cnt.limit(10).show()
    import priors.sparkSession.implicits._
    joinTB.rdd.map(x => {
      (x(0).toString, x(1).toString)
    }).groupByKey().mapValues(x => {
      val recodes = x.toSet
      (recodes.size, recodes.mkString(","))
    })
      .toDF("user_id", "tuple")
      .selectExpr("user_id", "tuple._1 as prod_dis_cnt", "tuple._2 as prod_recodes")
      .limit(10)
      .show()
    //每个用户购买的平均每个订单的商品数量（hive已做 过）
    //先求每个订单的商品
    val ord_pro: DataFrame = priors.groupBy("order_id").count() //得到的列名就是count
    //求每个用户订单中商品的平均值【对user做聚合，avg(商品个数)】
    orders.join(ord_pro, Seq("order_id", "order_id"))
      .groupBy("user_id").agg(avg("count"))
      .withColumnRenamed("avg(count)", "u_avg_ord_prods")
      .limit(10).show()
  }
}
