package sparkpack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object DataframeIntoRDDs {
 
 /*Task 1
   Add extra jar,read data.avro
  createtempview,once data is ready
  find a way to convert data into rdd to RDD[String]*/

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Dataframe").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder()
      .appName("spark sql basis ex")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    import spark.implicits._

    println("===========avro===========")
    val avrodf = spark.read.format("avro").option("header", "true")
      .load("file:///d:/Big data full stack/Spark_files/data/data.avro")
//    avrodf.show()

  /*  val convertedinto_rdd=avrodf.rdd
    convertedinto_rdd.foreach(println)*/
//        OR
    val rdd=avrodf.rdd.map(x=>x.toString())
    rdd.foreach(println)
    
  }
}