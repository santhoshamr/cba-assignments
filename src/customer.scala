import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, concat, length, lit, when}

//• for each combination of state, gender and age mask postcode of the customer if cell size is greater than 5 (masking sign ******)
//• for age column apply bucketing: 5 years buckets [20-24], [25-29], [30-34]...Example: age = 22 becomes [20-24], age = 35 becomes [35-39]. Bucketing is inclusive.
//• filter out customers younger than 20

object customer extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "customerTransactions")
  sparkConf.set("spark.master", "local[*]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

  val ageBucket = when(col("age").between(20,24), lit("[20-24]"))
    .when(col("age").between(25,29), lit("[25-29]"))
    .when(col("age").between(30,34), lit("[30-34]"))
    .when(col("age").between(35,39), lit("[35-39]"))
    .when(col("age").between(40,44), lit("[40-44]"))
    .when(col("age").between(45,49), lit("[45-49]"))
    .when(col("age").between(50,54), lit("[50-54]"))
    .when(col("age").between(55,59), lit("[55-59]"))
    .when(col("age").between(60,64), lit("[60-64]"))

  val customersDf = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("C:\\Users\\91961\\Desktop\\CBA\\program\\customer.csv")

  val custDf = customersDf
    //• filter out customers younger than 20
    .filter(col("age") >= 20)
    //• for each combination of state, gender and age mask postcode of the customer if cell size is greater than 5 (masking sign ******)
    .withColumn("postcode", when(length(concat(col("state"),col("gender"),col("age")))>5, lit("****")).otherwise(col("postcode")))
    //• for age column apply bucketing: 5 years buckets [20-24], [25-29], [30-34]...Example: age = 22 becomes [20-24], age = 35 becomes [35-39]. Bucketing is inclusive.
    .withColumn("AgeBucket", ageBucket )

  custDf.show(false)

  custDf.write
    .format("csv")
    //.partitionBy("AgeBucket")   //Using partitionBy to bucket customers to proper 5 years bucket for performance purpose
    .mode(SaveMode.Overwrite)
    .option("path", "C:/Users/91961/Desktop/CBA/Output/customer")
    .save()
}
