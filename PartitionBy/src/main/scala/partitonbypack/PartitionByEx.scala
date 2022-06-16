package partitonbypack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession


object PartitionByEx {
  
  def main(args: Array[String]): Unit = {
    
  val conf=new SparkConf().setAppName("partionby").setMaster("local[*]")
  val sc=new SparkContext(conf)
  val spark=SparkSession.builder().getOrCreate()
                import spark.implicits._
                
   val df=spark.read.format("csv").option("header", "true").load("file:///d:/Big data full stack/Spark_files/data/allCountry.csv")
    df.show()
    
   df.write.format("json").partitionBy("country").mode("overwrite").save("file:///d:/Big data full stack/Spark_files/data/partiondata")
  }
  
  
}