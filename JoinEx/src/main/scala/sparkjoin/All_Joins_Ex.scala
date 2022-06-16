package sparkjoin

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

object All_Joins_Ex extends App {

  val conf = new SparkConf().setAppName("Dataframe").setMaster("local[*]")
  val sc = new SparkContext(conf)
  sc.setLogLevel("ERROR")

  val spark = SparkSession.builder()
    .appName("spark sql basis ex")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()
  import spark.implicits._

  val df1 = spark.read.format("csv").option("header", "true").load("file:///d:/Big data full stack/Spark_files/data/jn1.txt")
  df1.show()
  val df2 = spark.read.format("csv").option("header", "true").load("file:///d:/Big data full stack/Spark_files/data/jn2.txt")
  df2.show()

  println("========Inner join===========")
  val joindata = df1.join(df2, Seq("id"), "inner")
  joindata.show()

  println("========Left join===========")
  val joinleft = df1.join(df2, Seq("id"), "left")
  joinleft.show()

   println("========Right join===========")
  val joinright = df1.join(df2, Seq("id"), "right")
  joinright.show()

   println("========Outer join===========")
  val joinouter = df1.join(df2, Seq("id"), "outer")
  joinouter.show()

   println("========Left anti join===========")
  val joinletanti = df1.join(df2, Seq("id"), "left_anti")
  joinletanti.show()
  
  
  val df3 = spark.read.format("csv").option("header", "true").load("file:///d:/Big data full stack/Spark_files/data/jn1.txt")
  df1.show()
  val df4 = spark.read.format("csv").option("header", "true").load("file:///d:/Big data full stack/Spark_files/data/jn3.txt")
  df2.show()
  
  println("======== when_idname_is_different===========")
  val when_idname_is_different=df3.join(df4,df3("id")===df4("custid"),"inner")
                              .drop("custid")
  when_idname_is_different.show()
  
  
  
  
  

}