package cn.inspur

import org.apache.spark.{SparkConf, SparkContext}

object Demo4 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("demo4").setMaster("local"))

    val rdd = sc.textFile("src/main/input/data1")

    val c_p_rdd = rdd.filter(line => !line.contains("child") && !line.contains("parent"))
      .map(_.split("\\s+"))
      .map(x=>(x(0),x(1)))
    val p_c_rdd=c_p_rdd.map(t=>(t._2,t._1))
    val newRdd=c_p_rdd.join(p_c_rdd)//父母（祖父母，孙子）
    newRdd.map(t=>(t._2._2,t._2._1)).saveAsTextFile("src/main/output//output4")
  }
}
