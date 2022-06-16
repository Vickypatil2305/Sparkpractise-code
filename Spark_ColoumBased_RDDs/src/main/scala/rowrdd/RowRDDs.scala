package rowrdd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Rdds_Example {
  
  val conf=new SparkConf().setAppName("rdd").setMaster("local[*]")
  val sc=new SparkContext(conf)
  sc.setLogLevel("Error")
  
//  val rddString=
  
}