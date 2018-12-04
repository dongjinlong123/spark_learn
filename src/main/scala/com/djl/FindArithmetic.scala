package com.djl

/**
  * 常见的查找算法
  * 1. 冒泡法
  * 2. 排序法
  * 3. 二分法
  */
object FindArithmetic {

  /**
    * 冒泡法
    *
    * @param str
    * @return
    * 给你 n 个数 ，我们用第一个数和第二个数进行比较 ，大的那个就放第二位 ，
    * 然后第二位和第三位比较 ，大的那个放在第三位 ，依次比下去 ，
    * 待到和最后一个比较结束 ，我们就能得到这 n 个数中最大的那个 ，且放在最后一个位置上 。
    */
  def maoPao(arr: Array[Int]): Array[Int] = {
    for (i <- 0 until arr.length) {
      for (j <- 0 until arr.length - 1 - i) {
        if (arr(j) > arr(j + 1)) {
          val temp = arr(j)
          arr(j) = arr(j + 1)
          arr(j + 1) = temp
        }
      }
    }
    arr
  }

  /**
    * 快速排序
    *
    * @param arr
    * @return
    * 快速排序 ，快速排序是对冒泡排序的一种改进 。
    * 它的基本思想是将要排序的数据分割成独立的两部分 ，
    * 其中一部分的所有数据都比另外一部分的所有数据都要小 。
    * 然后在按照此方法对两部分数据分别进行快速排序 ，
    * 最终得到一个有序的序列 。
    */
  def paixu(arr: Array[Int]): Array[Int] = {
    for (i <- 0 until arr.length) {
      //第一次排序
      var k = i
      for (j <- k + 1 until arr.length) {
        //第二次排序
        if (arr(k) > arr(j)) {
          //前面一个大于后面一个 记下目前找到的最大值所在的位置
          k = j
        }
      }
      if (i != k) {
        val tmp = arr(i)
        arr(i) = arr(k)
        arr(k) = tmp
      }
    }
    arr
  }

  /**
    * 二分法查找
    * 一个数组，输入一个数字，查找该数字在数组中的下标。 如果找不到，返回-1
    *
    * @param arr
    * @param z
    * @return
    */
  def erfenFind(arr: Array[Int], num: Int): Int = {
    var start = 0; // 默认起始坐标
    var end = arr.length - 1; // 默认结尾坐标
    var index = -1; // 找不到默认index为-1
    while (start <= end) {
      val middle = (start + end) / 2; // 计算中间下标
      if(num == arr(middle)){
        index = middle
        return index
      }
      if(num > arr(middle)){
        start = middle + 1
      }

      if(num < arr(middle)){
        end = middle - 1
      }
    }
    index
  }

  def main(args: Array[String]): Unit = {
    val arr = Array(1, 4, 5, 2, 3, 6, 1, 2, 44, 62, 3, 12, 312, 313, 3)
    println(maoPao(arr).toBuffer)
    println(paixu(arr).toBuffer)
    println(erfenFind(arr,44))
  }
}
