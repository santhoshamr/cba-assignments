import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{broadcast, col, date_format, sum, to_date}

//• filter out customers that did not transact at all
//• filter out transactions of customers that do not present in the customer dataset after previous treatments
//• provide summary of spendings by days of week: how much customers spend on Mondays, Tuesdays ...Sundays

object customerTransactions extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "customerTransactions")
  sparkConf.set("spark.master", "local[*]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

  val customersDf = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("C:\\Users\\91961\\Desktop\\CBA\\program\\customer.csv")

  val transactionsDf = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("C:\\Users\\91961\\Desktop\\CBA\\program\\transactions.csv")

  //filter out customers that did not transact at all
  //filter out transactions of customers that do not present in the customer dataset after previous treatments
  //Here inner join satisfies above 2 conditions and using BROADCAST join for optimization
  val custTransDf=transactionsDf.join(broadcast(customersDf), transactionsDf.col("CustomerID")===customersDf.col("person_id"), "inner")

  custTransDf.show(false)

  //provide summary of spendings by days of week: how much customers spend on Mondays, Tuesdays ...Sundays
  val transactionsByDaysDf=transactionsDf
    .withColumn("Year", date_format(to_date(col("Date"), "MM/dd/yyyy"), "yyyy"))
    .withColumn("Month1", date_format(to_date(col("Date"), "MM/dd/yyyy"), "MM"))
    .withColumn("Week", date_format(to_date(col("Date"), "MM/dd/yyyy"), "W"))
    .withColumn("Day", date_format(to_date(col("Date"), "MM/dd/yyyy"), "E"))
    .groupBy("Year", "Month1", "Week", "Day")
    .agg(sum("Total"))
    .orderBy("Year", "Month1", "Week", "Day")

  transactionsByDaysDf.show(1000,false)

  transactionsByDaysDf.write
    .format("csv")
    .mode(SaveMode.Overwrite)
    .option("path", "C:/Users/91961/Desktop/CBA/Output/transactionsByDays")
    .save()

}
