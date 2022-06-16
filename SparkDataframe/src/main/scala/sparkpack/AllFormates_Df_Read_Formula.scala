package sparkpack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object DataframeFormula {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Dataframe").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder()
      .appName("spark sql basis ex")
      .getOrCreate()
    import spark.implicits._

    println("===========csv===========")
    val csvdf = spark.read.format("csv").option("header", "true").load("file:///d:/Big data full stack/Spark_files/data/usdata.csv")
    csvdf.show(5)

    println("===========json===========")
    val jsondf = spark.read.format("json").load("file:///d:/Big data full stack/Spark_files/data/devices.json")
    jsondf.show(5)

    println("===========parquet===========")
    val parquetdf = spark.read.format("parquet").load("file:///d:/Big data full stack/Spark_files/data/data.parquet")
    parquetdf.show(5)

    println("===========orc===========")
    val orcdf = spark.read.format("orc").load("file:///d:/Big data full stack/Spark_files/data/orcdata.orc")
    orcdf.show(5)

    println("===========avro===========")
    val avrodf = spark.read.format("avro").load("file:///d:/Big data full stack/Spark_files/data/data.avro")
    avrodf.show(5)

    
    println("===========done===========")

//    avrodf.write.format("parquet").mode("overwrite").save("file:///d:/Big data full stack/Spark_files/data/pardavrodata")

  }

}