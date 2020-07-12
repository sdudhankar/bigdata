package org.retail

import org.apache.spark.{SparkConf,SparkContext}

object GetRevenuePerOrder {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster(args(0)).setAppName("Get Revenue Per Order")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val revenue = sc.textFile(args(1)) //your input file location
      .map(line=>(line.split(",")(1).toInt,line.split(",")(4).toFloat))
      .reduceByKey(_+_).sortByKey()

      revenue.coalesce(1).saveAsTextFile("output/GetRevenue")

    revenue.foreach(x=>println(x._1+","+x._2))
  }
}
