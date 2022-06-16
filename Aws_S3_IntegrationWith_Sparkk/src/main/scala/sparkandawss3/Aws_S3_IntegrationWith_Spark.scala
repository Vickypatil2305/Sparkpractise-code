package sparkandawss3

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

object Aws_S3_IntegrationWith_Spark extends App {

  /* There steps to intergrate s3 with spark :-
  *
  * 1. Add aws jars (aws java sdk jar)
  * 2. pass accesskey and secretkey in config()
  * 3. read from s3 location
  */

  println("====started=====")

  val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    .set("fs.s3a.access.key", "AKIA2RDQ4DHZL6CICLEO")
    .set("fs.s3a.secret.key", "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")

  val sc = new SparkContext(conf)
  sc.setLogLevel("ERROR")

  val spark = SparkSession.builder().config(conf).getOrCreate()
  import spark.implicits._

   println("====Aws S3 data=====")
  val df = spark.read.format("csv").option("header", "true").load("s3a://com.zeyo.dev/txns10k.txt")
  spark.time(df.show(80));

}