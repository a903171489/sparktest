package cn.inspur

import org.apache.spark.sql.SparkSession
//创建一个样例类
case class Person(name:String,age:Int)

object sqlDemo {
  def main(args: Array[String]): Unit = {

      //创建spark Session对象
    val sess = SparkSession.builder().appName("sqldemo").master("local").getOrCreate()

    val lst = List(Person("jack",20),Person("zzj",22),Person("syr",21),Person("bbq",15))

    val df = sess.createDataFrame(lst)



    df.select("name").where("age == 15").show()
  }
}
