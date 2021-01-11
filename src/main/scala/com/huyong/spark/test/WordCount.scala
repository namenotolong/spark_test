package com.huyong.spark.test

import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

class WordCount {

}
object WordCount{
  def main(args: Array[String]): Unit = {
    val fileName = "/Users/huyong/Desktop/workHome/test.txt";
    val context = getContext
    val rdd = context.textFile(fileName)
    val value = rdd.flatMap(e => e.split(" ")).map((_, 1))
    val tuples = value.reduceByKey(_ + _).collect()
    tuples foreach println
  }

  def getContext: SparkContext = {
    new SparkContext(new SparkConf().setMaster("local[*]").setAppName("wordCount"))
  }

  def testBroadCast(): Unit = {
    val list : List[Int] = List(1,2,3,4)
    val sc = getContext
    val value = sc.broadcast(list)
  }

  def testAccumulator(): Unit = {
    val sc = getContext
    val accumulator = new LongAccumulator()
    sc.register(accumulator, "accumulator")
    accumulator.add(1)
  }

  private lazy val a : Int = {
    1 + 2
  }



}

