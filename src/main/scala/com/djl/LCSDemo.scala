package com.djl

object LCSDemo {
  def main(args: Array[String]): Unit = {
    val str1 = "acdfg"
    val str2 = "adfc"
    println(lcs(str1, str2))
  }

  def lcs(str1: String, str2: String): Double = {
    //Array.ofDim[Int](x,y) 生成一个二维数组初始值为Int类型，并且维度为x 和 y
    val list_arr = Array.ofDim[Int](str1.length + 1, str2.length + 1)
    for (i <- 1 to str1.length; j <- 1 to str2.length) {
      if (str1(i - 1) == str2(j - 1)) {
        list_arr(i)(j) = list_arr(i - 1)(j - 1) + 1
      } else {
        list_arr(i)(j) = list_arr(i)(j - 1).max(list_arr(i - 1)(j))
      }
    }
    val ret = list_arr(str1.length)(str2.length)
    ret * 2 / (str1.length + str2.length).toDouble
  }

  def lcs2(a: String, b: String): Double = {
    val opt = Array.ofDim[Int](a.length + 1, b.length + 1)
    for (i <- 0 until a.length reverse) {
      for (j <- 0 until b.length reverse) {
        if (a(i) == b(j)) opt(i)(j) = opt(i + 1)(j + 1) + 1
        else opt(i)(j) = opt(i + 1)(j).max(opt(i)(j + 1))
      }
    }
    opt(0)(0) * 2 / (a.length + b.length).toDouble
  }
}
