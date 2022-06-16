package sparkdsl

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object ColFuntionsEx {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Dsl").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val df = spark.read.format("csv").option("header", "true").load("file:///d:/Big data full stack/Spark_files/data/usdata.csv")
    df.persist()    
    df.show(5)

    val firstlastdata = df.select("first_name", "last_name")
    //    firstlastdata.show()

    val data = df.filter("state='LA'");
    //     data.show()

    //      $"col" or col("col")
    //    val data1=df.filter($"first_name" ==="James")
    //     OR
    val data1 = df.filter(col("first_name") === "James")
    //    data1.show()

    val multiData = df.filter(col("first_name") === "Donette" && col("last_name") === "Foller")
    //    multiData.show()

    val multiValue = df.filter(col("first_name").isin("James", "Art"))
    //    multiValue.show();

    val filter = df.filter(col("first_name").isin("James", "Art") && col("last_name").isin("Butt", "Venere"))
    //    filter.show()

    val notEquals = df.filter(col("first_name").notEqual("James"))
    //    notEquals.show()

    val fewColoums = df.filter(col("first_name") === "James").select("last_name")
    //   fewColoums.show()

    val lastNo = df.selectExpr("first_name", "last_name", "company_name", "address",
      "city", "county", "state", "zip", "age",
      "split(phone1, '-')[2] as lastNo",
      "phone2", "email", "web")
    //   lastNo.show()

  }
}