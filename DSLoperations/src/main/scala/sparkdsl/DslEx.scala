package sparkdsl

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object DslEx {

  /*1)filter country='IND'
2) filter country IND and spendby cash
3) filter country IND or spendby cash
4) filter country no equals to IND
5) filter country in IND and US
6) filter country not in IND and US
7) Filter country like IN*/

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Dsl").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val df = spark.read.format("csv").option("header", "true").load("file:///d:/Big data full stack/Spark_files/data/allcountry.csv")
    df.persist()
    //    df.show(5)

    val filter = df.filter(col("country") === "IND")
    //    filtercont.show()

    println("--------------------")
    val filterAnd = df.filter(col("country") === "IND" && col("spendby") === "cash")
    //    filterci.show()

    val filterOr = df.filter(col("country") === "IND" || col("spendby") === "cash")
    //    filter.show()

    val filternoteq = df.filter(!(col("country") === "IND"))
    //    filternoteq.show()

    val filterIsIn = df.filter(col("country") isin ("IND", "US"))
    filterIsIn.show()

    val filterNotIsIn = df.filter(!(col("country") isin ("IND", "US")))
    filterNotIsIn.show()

    val filterLike = df.filter(col("country") like ("%IN%"))
    filterLike.show()

    val filNotLike = df.filter(!(col("country") like ("%IN%")))
    filNotLike.show()

  }
}