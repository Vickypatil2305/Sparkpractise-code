package sparkdsl

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object SplitExpr {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Dsl").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val df = spark.read.format("csv").option("header", "true").load("file:///d:/Big data full stack/Spark_files/data/allcountry.csv")
    df.persist()
    //    df.show()

    val year = df.selectExpr(
      "id",
      "split(tdate,'-')[2] as year",
      "LOWER(name) as first_name", //To lower case the name
      "check",
      "spendby",
      "initCap(country) as Country", // To upper case the country
      "case when spendby='cash' then 0 else 1 end as Status")
    //    year.show()

      
//                  TASKS
    /*Task 1 ---
        Today you achieved year with Split
        Find a way to achieve with timestamp methods from_unixtime(unix_timestamp(     ))
		*/

    /*	Task 2 ---
        read allcountry.csv
        Replace check columns with I - INDIA ,K - UnitedK ,S-USA */
    val df1 = spark.read.format("csv").option("header", "true").load("file:///d:/Big data full stack/Spark_files/data/allcountry.csv")
    df1.persist()
    val dfReplace_check = df1.selectExpr(
      "id",
      "split(tdate,'-')[2] as year",
      "LOWER(name) as first_name", //To lower case the name
      "case when check='I' then 'INDIA' when check='K' then 'Uinitedk' when check='S' then 'USA' end as Check",
      "spendby",
      "initCap(country) as Country", // To upper case the country
      "case when spendby='cash' then 0 else 1 end as Status")
    dfReplace_check.show()
    /*	Task 3 ---
				After task 2.. Find a way to order the dataframe desc with name column
		*/
  
        

  }
}