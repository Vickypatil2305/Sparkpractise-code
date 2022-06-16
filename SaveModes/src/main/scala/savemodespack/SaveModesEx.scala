package savemodespack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object SaveModesEx {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Dataframe").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder()
      .appName("spark sql basis ex")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    import spark.implicits._

    val df = spark.read.format("csv").option("header", "true").load("file:///d:/Big data full stack/Spark_files/data/txns.txt")
    df.show()

    df.write.format("csv").save("file:///C:/data/datawritescsv") // -- new directory
    //   OR
    //    df.write.format("csv").mode("error").save("file:///C:/data/datawritescsv")// - -already exist

    //    df.write.format("csv").mode("append").save("file:///C:/data/datawritescsv") //-- append the data

    //    df.write.format("csv").mode("overwrite").save("file:///C:/data/datawritescsv") //-- overwrites the data

    //    df.write.format("csv").mode("ignore").save("file:///C:/data/datawritescsv")// -- no change time of file generated

    println("===========df===========")
    /*avrodf.write.format("parquet").mode("overwrite").save("file:///d:/Big data full stack/Spark_files/data/pardavrodata")
    avrodf.write.format("parquet").mode("overwrite").save("file:///D:/data/writedf/parquetdata")
    avrodf.write.format("json").mode("overwrite").save("file:///D:/data/writedf/jsondata")
    avrodf.write.format("orc").mode("overwrite").save("file:///D:/data/writedf/orcdata")
    avrodf.write.format("avro").mode("overwrite").save("file:///D:/data/writedf/avrodata")
*/
  }
}