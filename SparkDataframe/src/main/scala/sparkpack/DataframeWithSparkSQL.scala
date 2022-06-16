package sparkpack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object Dataframe {

  def main(args: Array[String]): Unit = {

    //    satatFilter()
    delimiter()
  }

  def satatFilter() {
    val conf = new SparkConf().setAppName("Data Frame").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._ //it is part of sparksession

    val df = spark.read.format("csv").option("header", "true").load("file:///d:/Big data full stack/Spark_files/data/usdata.csv")
    df.show()

    df.createOrReplaceTempView("txnsDB")
    val gymdata = spark.sql("select * from txnsDB where state='LA'")
    gymdata.show()

  }

  
  def delimiter() {
    val conf = new SparkConf().setAppName("Data Frame").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val df = spark.read.format("csv").option("header", "true").load("file:///d:/Big data full stack/Spark_files/data/txnsheader.txt")
    df.show()

    val db = df.createOrReplaceTempView("TxnsDB")
    val gym = spark.sql("select * from TxnsDB where category like '%Exer%' ")
    gym.show()

    
    //   df.write.format("parquet").save("file:///d:/gymdaaata") // this commnd won't work because WinUtils not installed

    println("===========done============")

  }
}