
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._  // For DataFrame functions and expressions
import org.apache.spark.sql.types._     // For DataFrame schema types

object df_c_9 {
  def main(args:Array[String]):Unit={
    val ss = SparkSession.builder().appName("df_c_1").master("local[*]").getOrCreate()
    import ss.implicits._
    val payments = List(
      (1, "2024-07-15"),
      (2, "2024-12-25"),
      (3, "2024-11-01")
    ).toDF("payment_id", "payment_date")

    val df1 = payments.withColumn("quarter",when(month(col("payment_date")) isin(1,2,3),"Q1")
      .when(month(col("payment_date")) isin(3,4,5),"Q2")
      .when(month(col("payment_date")) isin(6,7,8),"Q3")
      .otherwise("Q4")
    )

    df1.show()




  }

}
