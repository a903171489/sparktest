package cn.inspur

import org.apache.spark.{SparkConf, SparkContext}

object Demo {
  def main(args: Array[String]): Unit = {
    //实例化SparkConf
    val conf = new SparkConf()
    conf.setAppName("demo")

    //实例化SparkContext
    val sc = new SparkContext(conf)
    val lit =List(11,55,88,61,49,47,26,49,83,59)
    //  val rdd = sc.atextFile("a.txt")

    val rdd = lit
    val rdd1 = rdd.sortBy(x=>x)
    println(rdd1)
  }

}
