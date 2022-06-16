package sparkpack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object Datafram_typesOfProcessing {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("First").setMaster("local[*]");
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val a = spark.read.format("csv").load("file:///d:/Big data full stack/Spark_files/data/txns.txt")
    a.show()
  }

}