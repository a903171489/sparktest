package cn.inspur

import org.apache.spark.{SparkConf, SparkContext}

object Demo2 {
  def main(args: Array[String]): Unit = {
    //实例化SparkConf and SparkContext
    val conf = new SparkConf()
    conf.setAppName("demo1").setMaster("local")
    val sc = new SparkContext(conf)

    val rdd = sc.textFile("src/main/input/a.txt")

    val rdd1 = rdd.flatMap(_.split(","))
            .foreach(println)

  }
}
