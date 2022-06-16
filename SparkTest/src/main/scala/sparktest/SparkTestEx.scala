package sparktest

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

object SparkTestEx extends App {

  val conf = new SparkConf().setAppName("Dataframe").setMaster("local[*]")
  val sc = new SparkContext(conf)
  sc.setLogLevel("ERROR")

  val spark = SparkSession.builder()
    .appName("spark sql basis ex")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()
  import spark.implicits._

  /* val df = spark.read.format("csv").option("header", "true").load("file:///d:/Big data full stack/Spark_files/data/allcountry.csv")
    df.show()

    val year=df.withColumn("tdate", expr("split(tdate,'-')[2]"))
               .withColumnRenamed("tdate", "year")
               .withColumn("status", expr("case when spendby='cash' then 1  else 2 end "))
    year.show()

    val cancatnate=df.withColumn("name_country", expr("concat(name,'-',country)"))
                     .drop("name")
                     .drop("country")
    cancatnate.show()*/

  /*
    val df1=spark.read.format("csv").option("header", true).load("file:///d:/Big data full stack/Spark_files/data/usdata.csv")
    df1.show()

    val location=df1.withColumn("location", expr("concat(city,' - ',state)"))
                    .drop("city")
                    .drop("state")
    location.show()
    */

  /* val df2=spark.read.format("csv").option("header", true).load("file:///d:/Big data full stack/Spark_files/data/usdata.csv")
    df2.show()

    val city=df2.withColumn("city",expr("concat(city,' - ',state)"))
                .withColumnRenamed("city", "location")
                .drop("state")
    city.show()*/

  /*
    val df3 = spark.read.option("header", true).csv("file:///d:/Big data full stack/Spark_files/data/zeyodata1.txt")
    df3.show()

    val groupbyAgg = df3.groupBy("name").agg(sum("amount") as ("Total"));
    groupbyAgg.show()

    val groupbyAgg1 = df3.groupBy("name").agg(sum("amount").cast(IntegerType).as("Total"));
    groupbyAgg1.show()
	*/

  /*
   val df4 = spark.read.option("header", true).csv("file:///d:/Big data full stack/Spark_files/data/zeyodata2.txt")
    df4.show()

    val groupbyagg2=df4.groupBy("name").agg(sum("amount").cast(IntegerType).as("Total")
                        ,count("product").as ("Total Product"))

    groupbyagg2.show()
    */

  /* val df5 = spark.read.option("header", true).csv("file:///d:/Big data full stack/Spark_files/data/zeyodata3.txt")
    df5.show()

    val groupbyagg3=df5.groupBy("name").agg(sum("amount").as("Total_No"),count("product").as("Product_count")).orderBy("Total");
    groupbyagg3.show()

    */

  //    =====================================================================

  /* val df = spark.read
                .format("csv")
                .option("header", "true")
                .csv("file:///d:/Big data full stack/Spark_files/data/allcountry.csv")
  spark.time(df.show())

  val filter_gymdata = df.filter(col("name") === "Sai")
  filter_gymdata.show()

  val monthOfFilterGymData = filter_gymdata.withColumn("tdate", expr("split(tdate,'-')[2] "))
    .withColumnRenamed("tdate", "month")
    .withColumn("status", expr("case when spendby='cash' then 1 else 2 end"))
  monthOfFilterGymData.show()

  val concatColumn = df.withColumn("country", expr("concat('spendby',' - ','country')"))
    .drop("spendby")*/

  //    concatColumn.show()

  /*  val df=spark.read.format("csv").option("header", "true").csv("file:///d:/Big data full stack/Spark_files/data/zeyodata1.txt")
    df.show()

    val groupOfData=df.groupBy("name").agg(sum("amount").cast(IntegerType)as "Total")
    groupOfData.show()

    */
  /*val df = spark.read.format("csv").option("header", "true").csv("file:///d:/Big data full stack/Spark_files/data/txns_head1.txt")
    df.show()
    val groupOfDatatxns = df.filter(col("product") === "Gymnastics Rings")
      .groupBy("product")
      .agg(min("amount") as "Lowest Price", max("amount") as "Highest Price", count("amount") as "Total No of Product", sum("amount") as "Total amount of all product")
    groupOfDatatxns.show()*/

  /*  Problem Statement:
Spark:
1. Create two data frame Employee(Employee id, Employee name, Department ID) and Depatment(Department id , department name).(attaching schema)
2. Join these based on Department id and extract  Employee id, Employee name, Department Name
3. Filter record from step 2 where department id = 100
*/

 /* val df1 = spark.read.format("csv").option("header", "true").load("file:///d:/Big data full stack/Spark_files/data/jn1.txt")
  df1.show()
  val df2 = spark.read.format("csv").option("header", "true").load("file:///d:/Big data full stack/Spark_files/data/jn2.txt")
  df2.show()

  val joindata = df1.join(df2, Seq("id"), "inner")
  joindata.show()

  val joinleft = df1.join(df2, Seq("id"), "left")
  joinleft.show()

  val joinright = df1.join(df2, Seq("id"), "right")
  joinright.show()

  val joinouter = df1.join(df2, Seq("id"), "outer")
  joinouter.show()

  val joinletanti = df1.join(df2, Seq("id"), "left_anti")
  joinletanti.show()*/
  
  /*val list=List(["1","ram","26"],["1","ram","26"],["1","ram","26"],["1","ram","26"],["1","ram","26"],["1","ram","26"])
  
val list=List(1,2,3)*/
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
}