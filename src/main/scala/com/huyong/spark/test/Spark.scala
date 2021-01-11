package com.huyong.spark.test

import com.huyong.spark.test.utils.SparkUtils
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, Row, SparkSession, functions}
import org.apache.spark.sql.functions.{approx_count_distinct, avg, broadcast, count, countDistinct, expr, first, last, lit, sum, sumDistinct}
import org.apache.spark.sql.types.{DataType, IntegerType, LongType, StringType, StructField, StructType}
//dataFrame相对于rdd优势   底层逻辑优化、不用产生jvm对象、减少序列化
//dataFrame => dataSet[Row]

object Spark {
  /**
   * 自定义udf
   * @param num
   * @return
   */
  def power3(num : Int): Int = {
    num * num * num
  }

  /**
   * 列基本操作：删除、重命名、增加、字面量、过滤、追加
   * rdd 分区 充分区 合并
   * 自定义udf函数 全局视图
   */
  def baseUse(): Unit = {
    val session = SparkUtils.createSparkSession()
    val schemas = StructType(Seq(StructField("name", StringType, nullable = true),
      StructField("age", LongType, nullable = true),
      StructField("address", StringType, nullable = true)))
    val frame = session.read.schema(schemas).json("test.txt")
    //重命名
    frame.withColumnRenamed("name", "rename").show()
    //复制一列，改变类型
    frame.withColumn("called", frame.col("age").cast("integer")).show()
    //删除列
    frame.withColumn("called", frame.col("age")).drop("called").show()
    //字面量转化为spark列
    frame.withColumn("litColumn", lit(1)).show()
    //查看某一列并重命名
    frame.select(frame.col("name")).as("test").show()
    //expr表达式
    frame.selectExpr("age + 10").show()
    frame.select(expr("age + 10")).show()
    //条件过滤
    frame.where("age > 10").show()
    //排序
    frame.sort("age").show()
    //创建临时视图
    frame.createOrReplaceTempView("user")
    session.sql("select * from user").show()
    //追加行
    val newRows = Seq(Row("xiaoming", 20L, "mianyang"), Row("mazi", 18L, "hangzhou"))
    val appendFrame = session.createDataFrame(session.sparkContext.parallelize(newRows), schemas)
    frame.union(appendFrame).show()
    println(frame.rdd.partitioner)
    println(frame.rdd.getNumPartitions)
    println(frame.rdd.getStorageLevel)
    println(frame.rdd.getCheckpointFile)
    //重新分区
    frame.repartition(2)
    //根据某个列固定分区、合并分区
    frame.repartition(5, frame.col("name")).coalesce(2)
    println(frame.rdd.getNumPartitions)
    //使用udf函数
    session.udf.register("power3", power3(_ : Int))
    frame.selectExpr("power3(age)").show()
  }

  /**
   * 聚合操作使用
   */
  def aggregateUse(): Unit = {
    val session = SparkUtils.createSparkSession()
    val schemas = StructType(Seq(StructField("name", StringType, nullable = true),
      StructField("age", IntegerType, nullable = true),
      StructField("address", StringType, nullable = true)))
    val frame = session.read.schema(schemas).json("test.txt")
    //count
    frame.select(count("name")).show()
    //count distinct
    frame.select(countDistinct("name")).show()
    //approx_count_distinct:处理大数据，近似值 rsd 可允许的误差
    frame.select(approx_count_distinct("name", 0.1)).show()
    //first last
    frame.select(first("name"), last("name")).show()
    //min max
    frame.select(functions.min("age"), functions.max("age")).show()
    //sum
    frame.select(sum("age")).show()
    //sumDistinct
    frame.select(sumDistinct("age")).show()
    //avg
    frame.select(avg("age")).show()
    //分组
    frame.groupBy("name").agg(count("age")).show()
    //使用map进行分组
    frame.groupBy("name").agg("age" -> "sum", "name" -> "count").show()
    //windows 函数、分组集、透视转换
    //方差、标准差、协方差、相关性
    //自定义聚合函数 UDAF
    /*session.udf.register("mySum", new MySum)
    frame.groupBy("name").agg("age" -> "mySum").show()*/
    session.udf.register("myCount", functions.udaf(new MyCount))
    frame.groupBy("name").agg("age" -> "myCount").show()
  }

  /**
   * inner join、outer join、 left outer join、 right outer join、
   * left semi join、left anti join、natual join、cross join
   */
  def joinUse(): Unit = {
    val session = SparkUtils.createSparkSession()
    val schemas = StructType(Seq(StructField("name", StringType, nullable = true),
      StructField("age", IntegerType, nullable = true),
      StructField("address", StringType, nullable = true)))
    val one = session.read.schema(schemas).json("test.txt")
    val two = session.read.schema(schemas).json("test.txt")
    one.createOrReplaceTempView("one")
    two.createOrReplaceTempView("two")
    session.sql("select * from one a inner join two b on a.name = b.name").explain()
    val express = one.col("name") === two.col("name");
    one.join(two, express, "inner").show()
    one.join(two, express, "outer").show()
    one.join(two, express, "right_outer").show()
    one.join(two, express, "left_outer").show()
    //相当于in
    one.join(two, express, "left_semi").show()
    //相当于not in
    one.join(two, express, "left_anti").show()
    one.join(two, express, "cross").show()
    //打标与小表join，将小表进行广播
    //指定广播并不是强制性，优化器有可能对其进行忽略
    one.join(two, express, "inner").explain()
    one.join(broadcast(two), express, "inner").explain()
    session.sql("select /*+ MAPJOIN(two) */ * from one join two on one.name = two.name").explain()
    //小表与小表的join 最好让spark来决定  但可以强制广播
    session.catalog.listTables().foreach(e => println(e.toString()))
  }

  /**
   * dataSet test
   */
  def dataSetTest(): Unit = {
    val session = SparkUtils.createSparkSession()
    val schemas = StructType(Seq(StructField("name", StringType, nullable = true),
      StructField("age", IntegerType, nullable = true),
      StructField("address", StringType, nullable = true)))
    val frame = session.read.schema(schemas).json("test.txt")
    import session.implicits._
    val users = frame.as[User]
    println(users.getClass)
    users.show()
  }
  def main(args: Array[String]): Unit = {
    dataSetTest()
  }

  def testAggregator(): Unit = {
    val list : List[Int] = List(1,2,3,4,5)
    val a = list.aggregate(0)(_ + _, _ + _)
    println(a)
  }
  def test(): Int = {
    2
  }
}

case class User(name : String, age : Long, address : String)

/**
 * spark3.0之前自定义UDAF
 */
class MySum extends UserDefinedAggregateFunction {
  /**
   * 入参类型
   * @return
   */
  override def inputSchema: StructType = StructType(StructField("age", LongType) :: Nil)

  /**
   * 中间结果类型
   * @return
   */
  override def bufferSchema: StructType = StructType(StructField("age", LongType) :: Nil)

  /**
   * 返回结果类型
   * @return
   */
  override def dataType: DataType = StructType(StructField("age", LongType) :: Nil)

  /**
   * 对于相同参数是否会返回相同的结果
   * @return
   */
  override def deterministic: Boolean = true

  /**
   * 初始化整个聚合缓冲区值
   * @param buffer
   */
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
  }

  /**
   * 给点行更新内部缓冲区
   * @param buffer
   * @param input
   */
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getLong(0) + input.getLong(0)
  }

  /**
   * 合并两个聚合缓冲区
   * @param buffer1
   * @param buffer2
   */
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
  }

  /**
   * 生成聚合的最终结果
   * @param buffer
   * @return
   */
  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0)
  }
}

/**
 * spark3.0之后的UDAF
 */
class MyCount extends Aggregator[Long, Long, Long] {
  override def zero: Long = 0L

  override def reduce(b: Long, a: Long): Long = a + b

  override def merge(b1: Long, b2: Long): Long = b1 + b2

  override def finish(reduction: Long): Long = reduction

  override def bufferEncoder: Encoder[Long] = Encoders.scalaLong

  override def outputEncoder: Encoder[Long] = Encoders.scalaLong
}
