package com.djl

class ScalaProgram {
  val name: String = "张三" // final  只提供get方法
  var age: Int = 22 // 提供get set
  private val height: Int = 180 //本类和伴生对象中使用
  private[this] var width: Int = 100 //只能本类中使用

  override def toString: String = {
    ScalaProgram.test() //调用伴生对象的静态方法
    "name" + this.name + "age" + this.age + "width" + this.width + "height" + height
  }

}

/*主构造器，构造方法*/
class myClass(val name: String, var age: Int, private val height: Int, width: Int) {

  //定义辅助构造器
  def this() {
    this("dd", 11, 123, 234)
  }

  override def toString: String = {
    "name" + this.name + "age" + this.age + "width" + this.width + "height" + height
  }

}

object ScalaProgram {
  private val CLASS_STATIC_FINAL_NAME="本类中才可以使用的静态常量"
  val STATIC_FINNAL_NAME="静态常量"
  def main(args: Array[String]): Unit = {
    val str = new ScalaProgram().toString
    println(str)
    val c1 = new myClass()
    val c2 = new myClass("aa", 1, 1, 1)
    println(c1.toString)
    println(c2.toString)
    //直接调用apply方法
    println(ScalaProgram(123,234))

  }

  //静态方法
  def test(): Unit = {
    println("test")
  }
  //apply方法
  def apply(x:Int,y:Int):Int={
    x+y
  }
}
