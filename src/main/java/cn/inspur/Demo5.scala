package cn.inspur

import org.apache.spark.{SparkConf, SparkContext}

object Demo5 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("demo4").setMaster("local[2]"))

    val rdd = sc.textFile("src/main/input/file1.txt,src/main/input/file2.txt")

    val arr=
      rdd
        .map(_.split(","))
        .map(line=>(line(2).toInt,line.toList))
        .sortByKey(false).take(10)

    sc.parallelize(arr).saveAsTextFile("src/main/output//output5")


  }
}
