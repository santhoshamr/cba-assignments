import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, concat, date_format, hour, length, lit, minute, rpad, sum, to_date, when}
import org.apache.spark.sql.types.IntegerType

//• correct or add loyalty flag (column loyal_customer, values true/false) using following
// conditions: if customer transacted on the amount greater than 1000 per month loyal_customer = true, otherwise false
// Note: each transaction total amount is in column Total
//• for transactions happening on Wednesdays cap the Total amount to 99 if the
//amount is more or equal to 100 (what do you do to the Tax, Unit price and
//Quantity columns in this case?)
//• for column Time in transactions round down by 15 min periods. Example:
//19:21 - becomes 19:15, 17:36 - becomes 17:30, 10:12 - becomes 10:00, 12:50 -
//becomes 12:45

object transactions extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "customerTransactions")
  sparkConf.set("spark.master", "local[*]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

  val transactionsDf = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("C:\\Users\\91961\\Desktop\\CBA\\program\\transactions.csv")

  val transMonthAgg  = Window.partitionBy("CustomerID", "Month")

  val transDf=transactionsDf
    .withColumn("Month", date_format(to_date(col("Date"), "MM/dd/yyyy"), "MM"))
    //if customer transacted on the amount greater than 1000 per month loyal_customer = true, otherwise false
    .withColumn("Sum_loyal_cust", sum(col("Total")).over(transMonthAgg))
    .withColumn("Loyal_cust", when(col("Sum_loyal_cust")>1000, lit("true")).otherwise("false"))
    //for column Time in transactions round down by 15 min periods. Example: 19:21 - becomes 19:15, 17:36 - becomes 17:30, 10:12 - becomes 10:00, 12:50 -becomes 12:45
    .withColumn("Time_New", concat(hour(col("Time")), lit(":"), rpad((minute(col("Time"))/15).cast(IntegerType)*15, 2, "0")))
    .withColumn("Day", date_format(to_date(col("Date"), "MM/dd/yyyy"), "E"))
    //• for transactions happening on Wednesdays cap the Total amount to 99 if the
    //amount is more or equal to 100, what do you do to the Tax, Unit price and Quantity columns in this case?
    //I have masked Tax, Unit price and Quantity columns for transactions happening on Wednesdays and capped the Total amount to 99
    .withColumn("Total_New", when(col("Day")==="Wed" && col("Total") >=100, lit("99")).otherwise(col("Total")))
    .withColumn("Tax 5%", when(col("Day")==="Wed" && col("Total") >=100, lit("*****")).otherwise(col("Tax 5%")))
    .withColumn("Unit price", when(col("Day")==="Wed" && col("Total") >=100, lit("*****")).otherwise(col("Unit price")))
    .withColumn("Quantity", when(col("Day")==="Wed" && col("Total") >=100, lit("*****")).otherwise(col("Quantity")))
    .orderBy("CustomerID", "Month")

  transDf.show(false)

  transDf.write
    .format("csv")
    .mode(SaveMode.Overwrite)
    .option("path", "C:/Users/91961/Desktop/CBA/Output/transactions")
    .save()
}
