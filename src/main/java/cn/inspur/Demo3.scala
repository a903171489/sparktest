package cn.inspur

import org.apache.spark.{SparkConf, SparkContext}

object Demo3 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("demo3").setMaster("local"))

    val rdd = sc.textFile("src/main/input/demo33.txt")

    rdd.map(_.split("\\s+"))
      .map(line => (line(0), line(1)))
      .groupByKey()
      .mapValues(_.toList)
      .map(t => (t._1, t._2.sortBy(x => x)))
      .foreach(println)



  }
}
