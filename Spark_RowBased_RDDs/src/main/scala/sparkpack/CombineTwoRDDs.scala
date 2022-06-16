package sparkpack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object CombineTwoRDDs {
  
  def main(args: Array[String]): Unit = {
   
   val conf=new SparkConf().setAppName("write").setMaster("local[*]")
   val sc=new SparkContext(conf)
   sc.setLogLevel("ERROR")
   
   
   val readData=sc.textFile("file:///d:/Big data full stack/Spark_files/data/txns.txt")
   val filterGym=readData.filter(x=>x.contains("Gym"))   
   val filterSports=readData.filter(x=>x.contains("Sports"))
   
   val combine=filterGym.union(filterSports)
   combine.foreach(println)
  }
}