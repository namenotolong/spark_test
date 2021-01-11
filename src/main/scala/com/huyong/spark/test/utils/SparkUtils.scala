package com.huyong.spark.test.utils

import org.apache.spark.sql.SparkSession

object SparkUtils {
  def createSparkSession(appName : String = "default", config : Map[String, String] = null) : SparkSession = {
    val name = SparkSession.builder().appName(appName).master("local")
    if (config != null && config.nonEmpty) {
      config.foreach(e => {
        name.config(e._1, e._2)
      })
    }
    name.getOrCreate()
  }
}
