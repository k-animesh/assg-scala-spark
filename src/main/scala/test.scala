import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.col
object test {
def main(args:Array[String]): Unit = {
  val spark = SparkSession.builder().appName("test").master("local[*]").getOrCreate()

  val df = spark.read.format("csv")
    .option("path","E:/DataForCognos.csv")
    .option("header",true)
    .load()
  val df1 = df.select(col("City"),col("Stationid"),col("Truckid"),col("Wastecollected"),functions.when(col("Wastecollected")<32,"less").otherwise("more").alias("amount"))

  df1.show(5)

}
}

