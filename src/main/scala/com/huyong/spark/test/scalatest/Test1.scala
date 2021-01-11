package com.huyong.spark.test.scalatest

class Test1 {

}
class Person{}
case class Student(name : String, age : Int) extends Person
case class Teacher(name : String, age : Int) extends Person
object Test1 {
  def main(args: Array[String]): Unit = {
    val a : Person = Student("heh", 1)
    a match {
      case x @ Student(name, age) => println(s"$name is $age $x")
      case _ => println("none")
    }
  }
}
